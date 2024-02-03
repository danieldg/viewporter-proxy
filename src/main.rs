use std::{
    error::Error,
    future::poll_fn,
    os::fd::AsFd,
    os::fd::AsRawFd,
    os::unix::net::UnixStream,
    sync::{Arc, OnceLock},
    task,
};
use tokio::{io::unix::AsyncFd, sync::Notify};
use wayland_client::Dispatch as DispatchClient;
use wayland_client::{Connection, Proxy};
use wayland_server::Dispatch as DispatchServer;
use wayland_server::{DataInit, Display, GlobalDispatch, New, Resource};

use wayland_client::protocol::wl_callback as wlc_callback;
use wayland_client::protocol::wl_registry as wlc_registry;
use wayland_server::protocol::wl_compositor as wls_compositor;

type ClientQH = wayland_client::QueueHandle<State>;

#[derive(Debug)]
struct Registry;

#[derive(Debug)]
struct Fd(i32);

impl AsRawFd for Fd {
    fn as_raw_fd(&self) -> i32 {
        self.0
    }
}

#[derive(Debug)]
struct ClientIO {
    conn: Connection,
    flush: Notify,
    fd: AsyncFd<Fd>,
}

#[derive(Debug)]
struct State {
    dispatch_server: bool,
    client_io: Arc<ClientIO>,
    c_qh: ClientQH,
    c_registry: wlc_registry::WlRegistry,
    s_handle: wayland_server::DisplayHandle,
}

impl State {
    fn create_global<I>(&mut self, version: u32, name: u32)
    where
        I: wayland_server::Resource + 'static,
        State: GlobalDispatch<I, ProxiedGlobal>,
    {
        self.s_handle
            .create_global::<State, I, _>(version, ProxiedGlobal { name });
    }

    fn setup<S, C>(&self, init: &mut DataInit<'_, State>, new: New<S>, make_c: impl FnOnce(&S) -> C)
    where
        S: Resource + 'static,
        C: Proxy + Send + Sync + 'static,
        State: DispatchServer<S, Proxied<C>>,
    {
        let s_proxy = init.init(new, Proxied::new());
        let c_proxy = make_c(&s_proxy);
        s_proxy
            .data::<Proxied<C>>()
            .unwrap()
            .client
            .set(c_proxy)
            .unwrap();
    }
}

struct ProxiedGlobal {
    name: u32,
}

struct Proxied<I> {
    client: OnceLock<I>,
}

impl<I> Proxied<I> {
    fn new() -> Self {
        Self {
            client: Default::default(),
        }
    }
}

impl<I> std::ops::Deref for Proxied<I> {
    type Target = I;
    fn deref(&self) -> &I {
        self.client.get().unwrap()
    }
}

impl DispatchClient<wlc_registry::WlRegistry, Registry> for State {
    fn event(
        state: &mut State,
        _: &wlc_registry::WlRegistry,
        event: wlc_registry::Event,
        _: &Registry,
        _: &Connection,
        _: &ClientQH,
    ) {
        use wlc_registry::Event::*;
        match event {
            Global {
                name,
                interface,
                version,
            } => match &*interface {
                "wl_compositor" => {
                    state.create_global::<wls_compositor::WlCompositor>(version.max(6), name);
                }
                _ => {}
            },

            _ => {}
        }
    }
}

impl DispatchClient<wlc_callback::WlCallback, Registry> for State {
    fn event(
        state: &mut State,
        _: &wlc_callback::WlCallback,
        _: wlc_callback::Event,
        _: &Registry,
        _: &Connection,
        _: &ClientQH,
    ) {
        state.dispatch_server = true;
    }
}

impl GlobalDispatch<wls_compositor::WlCompositor, ProxiedGlobal> for State {
    fn bind(
        state: &mut State,
        _: &wayland_server::DisplayHandle,
        _: &wayland_server::Client,
        global: New<wls_compositor::WlCompositor>,
        name: &ProxiedGlobal,
        init: &mut DataInit<'_, State>,
    ) {
        state.setup(init, global, |s| {
            state
                .c_registry
                .bind(name.name, s.version(), &state.c_qh, s.clone())
        });
    }
}

macro_rules! define_proxy {
    (fix_pattern: ($($base:tt)*) $i:ident $arg:tt) => {
        $($base)* :: $i $arg
    };
    (fix_pattern: ($($base:tt)*) $i:ident) => {
        $($base)* :: $i
    };

    (closure: ($($arg:tt)*), send: ($v:expr), expr: ( $($y:tt)* )) => {
        {
            fn handle<F>(f: F) -> F where F: FnOnce($($arg)*) {
                f
            }
            handle($($y)*)
        }
    };
    (closure: ($($arg:tt)*), send: ($v:expr), expr: pass) => {
        {
            fn handle<F>(f: F) -> F where F: FnOnce($($arg)*) {
                f
            }
            handle($v)
        }
    };
    (closure: ($($arg:tt)*), send: ($v:expr), expr: { $($y:tt)* }) => {
        {
            fn handle<F>(f: F) -> F where F: FnOnce($($arg)*) {
                f
            }
            handle({
                $($y)*
                $v
            })
        }
    };

    (core :: $mod:ident :: $ty:ident,
        request: { $(($($req_struct:tt)*): $req_handler:tt),* $(,)? }
        event: { $(($($ev_struct:tt)*): $ev_handler:tt),* $(,)? }
    ) => {
        impl
            DispatchServer<
                wayland_server::protocol::$mod::$ty,
                Proxied<wayland_client::protocol::$mod::$ty>,
            > for State
        {
            fn request(
                _state: &mut State,
                _: &wayland_server::Client,
                _: &wayland_server::protocol::$mod::$ty,
                event: wayland_server::protocol::$mod::Request,
                _proxy: &Proxied<wayland_client::protocol::$mod::$ty>,
                _: &wayland_server::DisplayHandle,
                _init: &mut DataInit<'_, State>,
            ) {
                match event {
                    $(
                        define_proxy!(fix_pattern:
                            (wayland_server::protocol::$mod::Request)
                            $($req_struct)*
                        ) => {
                            define_proxy!(
                                closure: (&mut State, &Proxied<wayland_client::protocol::$mod::$ty>, &mut DataInit<'_, State>),
                                send: (move |_, proxy, _| {
                                    proxy.send_request(
                                        define_proxy!(fix_pattern: (wayland_client::protocol::$mod::Request) $($req_struct)*)
                                    ).unwrap();
                                }),
                                expr: $req_handler
                            )(_state, _proxy, _init);
                        }
                    )*
                    _ => unreachable!(),
                }
            }
        }

        impl
            DispatchClient<wayland_client::protocol::$mod::$ty, wayland_server::protocol::$mod::$ty>
            for State
        {
            fn event(
                _state: &mut State,
                _: &wayland_client::protocol::$mod::$ty,
                event: wayland_client::protocol::$mod::Event,
                _proxy: &wayland_server::protocol::$mod::$ty,
                _: &Connection,
                _: &ClientQH,
            ) {
                match event {
                    $(
                        define_proxy!(fix_pattern:
                            (wayland_client::protocol::$mod::Event)
                            $($ev_struct)*
                        ) => {
                            define_proxy!(
                                closure: (&mut State, &wayland_server::protocol::$mod::$ty),
                                send: (move |_, proxy| {
                                    proxy.send_event(
                                        define_proxy!(fix_pattern: (wayland_server::protocol::$mod::Event) $($ev_struct)*)
                                    ).unwrap();
                                }),
                                expr: $ev_handler
                            )(_state, _proxy);
                        }
                    )*
                    _ => unreachable!(),
                }
            }
        }
    };
}

define_proxy!(core::wl_callback::WlCallback,
    request: { }
    event: {
        (Done { callback_data }): pass,
    }
);

define_proxy!(core::wl_compositor::WlCompositor,
    request: {
        (CreateSurface { id }): (
            |state, proxy, init| state.setup(init, id, |s| proxy.create_surface(&state.c_qh, s.clone()))
        )
    }
    event: { }
);

fn c2s<S: Resource, C: Proxy + 'static>(s: S) -> C {
    C::clone(s.data::<Proxied<C>>().unwrap())
}

fn s2c<S: Resource + Send + Sync + 'static, C: Proxy>(c: C) -> S {
    S::clone(c.data::<S>().unwrap())
}

define_proxy!(core::wl_surface::WlSurface,
    request: {
        (Destroy): pass,
        (Attach { buffer, x, y }): {
            let buffer = buffer.map(c2s);
        },
        (Damage { x, y, width, height }): pass,
        (Frame { callback }): (
            |state, proxy, init| state.setup(init, callback, |s| proxy.frame(&state.c_qh, s.clone()))
        ),
        (SetOpaqueRegion { region }): {
            let region = region.map(c2s);
        },
        (SetInputRegion { region }): {
            let region = region.map(c2s);
        },
    }
    event: {
        (Enter { output }): {
            let output = s2c(output);
        },
        (Leave { output }): {
            let output = s2c(output);
        },
        (PreferredBufferScale { factor }): pass,
        (PreferredBufferTransform { transform }): {
            let transform = match transform {
                wayland_server::WEnum::Value(x) => x as u32,
                wayland_server::WEnum::Unknown(x) => x,
            }.into();
        }
    }
);

async fn run(s_stream: UnixStream) -> Result<(), Box<dyn Error>> {
    let c_conn = Connection::connect_to_env()?;
    let c_display = c_conn.display();
    let mut c_queue = c_conn.new_event_queue();
    let c_qh = c_queue.handle();
    let c_fd = c_conn.prepare_read().unwrap().connection_fd().as_raw_fd();
    let c_registry = c_display.get_registry(&c_qh, Registry);
    let _ = c_display.sync(&c_qh, Registry);

    let mut s_display = Display::new()?;
    let mut s_handle = s_display.handle();
    let _s_client = s_handle.insert_client(s_stream, Arc::new(()))?;
    let s_fd = AsyncFd::new(Fd(s_display.as_fd().as_raw_fd()))?;
    let client_io = Arc::new(ClientIO {
        conn: c_conn,
        flush: Notify::new(),
        fd: AsyncFd::new(Fd(c_fd))?,
    });
    let mut state = State {
        dispatch_server: false,
        client_io,
        c_qh,
        c_registry,
        s_handle,
    };

    poll_fn(|cx| {
        let _ = c_queue.poll_dispatch_pending(cx, &mut state)?;
        if state.dispatch_server {
            let ready = s_fd.poll_read_ready(cx)?;
            s_display.dispatch_clients(&mut state)?;
            s_display.flush_clients()?;
            if let task::Poll::Ready(mut g) = ready {
                g.clear_ready();
            }
        }
        task::Poll::Pending
    })
    .await
}

fn main() -> Result<(), Box<dyn Error>> {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;

    tokio::task::LocalSet::new().block_on(&rt, async move {
        let _ = std::fs::remove_file("/run/user/1000/wayland-vp");
        let listen = tokio::net::UnixListener::bind("/run/user/1000/wayland-vp")?;
        loop {
            let (stream, _) = listen.accept().await?;

            tokio::task::spawn_local(run(stream.into_std()?));
        }
    })
}
