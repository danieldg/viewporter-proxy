use std::{
    error::Error,
    future::poll_fn,
    io,
    os::fd::{AsFd, AsRawFd, OwnedFd},
    os::unix::net::UnixStream,
    sync::{Arc, Mutex},
    task,
};
use tokio::io::unix::AsyncFd;
use wayland_client::Dispatch as DispatchClient;
use wayland_client::{Connection, Proxy};
use wayland_server::{DataInit, Display, GlobalDispatch, New, Resource};

use wayland_client::protocol::wl_callback as wlc_callback;
use wayland_client::protocol::wl_registry as wlc_registry;
use wayland_server::protocol::wl_compositor as wls_compositor;
use wayland_server::protocol::wl_seat as wls_seat;

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
    fd: AsyncFd<Fd>,
}

#[derive(Debug)]
struct State {
    dispatch_server: bool,
    c_display: wayland_client::protocol::wl_display::WlDisplay,
    c_qh: ClientQH,
    c_backend: wayland_client::backend::Backend,
    c_registry: wlc_registry::WlRegistry,
    s_handle: wayland_server::DisplayHandle,
    s_client: wayland_server::Client,
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
}

struct ProxiedGlobal {
    name: u32,
}

struct Proxied {
    map_c2s: Option<ProxyC2S>,
    map_s2c: Option<ProxyS2C>,
    server: wayland_server::backend::WeakHandle,
    s_iface: &'static wayland_server::backend::protocol::Interface,
    inner: Mutex<ProxiedInner>,
}

type ProxyC2S = fn(
    &Mutex<ProxiedInner>,
    &wayland_server::backend::Handle,
    &mut State,
    &mut wayland_server::backend::protocol::Message<wayland_server::backend::ObjectId, OwnedFd>,
) -> Option<Arc<(dyn wayland_server::backend::ObjectData<State> + 'static)>>;
type ProxyS2C = fn(
    &Mutex<ProxiedInner>,
    &wayland_client::backend::Backend,
    &mut wayland_client::backend::protocol::Message<wayland_client::backend::ObjectId, OwnedFd>,
) -> Option<Arc<(dyn wayland_client::backend::ObjectData + 'static)>>;

struct ProxiedInner {
    client: wayland_client::backend::ObjectId,
    server: wayland_server::backend::ObjectId,
    version: u32,
}

impl Proxied {
    fn new(
        map_c2s: Option<ProxyC2S>,
        map_s2c: Option<ProxyS2C>,
        server: &wayland_server::backend::Handle,
        s_iface: &'static wayland_server::backend::protocol::Interface,
    ) -> Arc<Self> {
        Arc::new(Self {
            map_c2s,
            map_s2c,
            server: server.downgrade(),
            s_iface,
            inner: Mutex::new(ProxiedInner {
                client: wayland_client::backend::ObjectId::null(),
                server: wayland_server::backend::ObjectId::null(),
                version: 0,
            }),
        })
    }
}

impl wayland_server::backend::ObjectData<State> for Proxied {
    fn request(
        self: Arc<Self>,
        handle: &wayland_server::backend::Handle,
        state: &mut State,
        _: wayland_server::backend::ClientId,
        mut msg: wayland_server::backend::protocol::Message<
            wayland_server::backend::ObjectId,
            OwnedFd,
        >,
    ) -> Option<Arc<(dyn wayland_server::backend::ObjectData<State> + 'static)>> {
        use wayland_client::backend::protocol::Argument as CArg;
        use wayland_server::backend::protocol::Argument as SArg;
        let rv = if let Some(f) = self.map_c2s {
            f(&self.inner, handle, state, &mut msg)
        } else {
            None
        };
        let client_oid = state
            .c_backend
            .send_request(
                wayland_client::backend::protocol::Message {
                    sender_id: self.inner.lock().unwrap().client.clone(),
                    opcode: msg.opcode,
                    args: msg
                        .args
                        .iter()
                        .map(|x| match x {
                            SArg::Int(i) => CArg::Int(*i),
                            SArg::Uint(i) => CArg::Uint(*i),
                            SArg::Fixed(i) => CArg::Fixed(*i),
                            SArg::Str(s) => CArg::Str(s.clone()),
                            SArg::Object(i) => CArg::Object(
                                state
                                    .s_handle
                                    .backend_handle()
                                    .get_object_data::<State>(i.clone())
                                    .ok()
                                    .as_deref()
                                    .and_then(|d| d.downcast_ref::<Proxied>())
                                    .map_or(wayland_client::backend::ObjectId::null(), |p| {
                                        p.inner.lock().unwrap().client.clone()
                                    }),
                            ),
                            SArg::NewId(server_oid) => {
                                rv.as_ref()
                                    .and_then(|x| x.downcast_ref::<Proxied>())
                                    .map(|p| p.inner.lock().unwrap().server = server_oid.clone());

                                CArg::NewId(wayland_client::backend::ObjectId::null())
                            }
                            SArg::Array(a) => CArg::Array(a.clone()),
                            SArg::Fd(fd) => CArg::Fd(fd.as_raw_fd()),
                        })
                        .collect(),
                },
                rv.clone()
                    .and_then(|x| x.downcast_arc::<Proxied>().ok())
                    .map(|x| x as _),
                None,
            )
            .unwrap();
        rv.as_ref()
            .and_then(|x| x.downcast_ref::<Proxied>())
            .map(|p| {
                let mut lock = p.inner.lock().unwrap();
                lock.client = client_oid;
                lock.version = self.inner.lock().unwrap().version;
            });
        rv
    }
    fn destroyed(
        self: Arc<Self>,
        _: &wayland_server::backend::Handle,
        _: &mut State,
        _: wayland_server::backend::ClientId,
        _: wayland_server::backend::ObjectId,
    ) {
    }
}

impl wayland_client::backend::ObjectData for Proxied {
    fn event(
        self: Arc<Self>,
        handle: &wayland_client::backend::Backend,
        mut msg: wayland_client::backend::protocol::Message<
            wayland_client::backend::ObjectId,
            OwnedFd,
        >,
    ) -> Option<Arc<(dyn wayland_client::backend::ObjectData + 'static)>> {
        use wayland_client::backend::protocol::Argument as CArg;
        use wayland_server::backend::protocol::Argument as SArg;
        let rv = self
            .map_s2c
            .as_ref()
            .and_then(|f| f(&self.inner, handle, &mut msg));
        let sh = self.server.upgrade().unwrap();
        let sender_id = self.inner.lock().unwrap().server.clone();
        dbg!(&msg);
        sh.send_event(wayland_server::backend::protocol::Message {
            opcode: msg.opcode,
            args: msg
                .args
                .iter()
                .map(|x| match x {
                    CArg::Int(i) => SArg::Int(*i),
                    CArg::Uint(i) => SArg::Uint(*i),
                    CArg::Fixed(i) => SArg::Fixed(*i),
                    CArg::Str(s) => SArg::Str(s.clone()),
                    CArg::Object(i) => SArg::Object(
                        handle
                            .get_data(i.clone())
                            .ok()
                            .as_deref()
                            .and_then(|d| d.downcast_ref::<Proxied>())
                            .map_or(wayland_server::backend::ObjectId::null(), |p| {
                                p.inner.lock().unwrap().server.clone()
                            }),
                    ),
                    CArg::NewId(_) => {
                        let cli = sh.get_client(sender_id.clone()).unwrap();
                        SArg::NewId(
                            rv.clone()
                                .and_then(|x| x.downcast_arc::<Proxied>().ok())
                                .map_or(wayland_server::backend::ObjectId::null(), |p| {
                                    let mut lock = p.inner.lock().unwrap();
                                    if lock.server.is_null() {
                                        lock.server = sh
                                            .create_object::<State>(
                                                cli,
                                                p.s_iface,
                                                lock.version,
                                                p.clone(),
                                            )
                                            .unwrap();
                                    }
                                    lock.server.clone()
                                }),
                        )
                    }
                    CArg::Array(a) => SArg::Array(a.clone()),
                    CArg::Fd(fd) => SArg::Fd(fd.as_raw_fd()),
                })
                .collect(),
            sender_id,
        })
        .unwrap();
        rv
    }
    fn destroyed(&self, _: wayland_client::backend::ObjectId) {}
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
                "wl_seat" => {
                    state.create_global::<wls_seat::WlSeat>(version.max(8), name);
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
        dbg!();
        state.dispatch_server = true;
    }
}

impl GlobalDispatch<wls_compositor::WlCompositor, ProxiedGlobal> for State {
    fn bind(
        state: &mut State,
        handle: &wayland_server::DisplayHandle,
        _: &wayland_server::Client,
        global: New<wls_compositor::WlCompositor>,
        name: &ProxiedGlobal,
        init: &mut DataInit<'_, State>,
    ) {
        use wayland_client::protocol::wl_registry::Request;
        type I = wayland_client::protocol::wl_compositor::WlCompositor;
        let data = Proxied::new(
            Some(|proxy, handle, state, msg| {
                use wayland_server::protocol::wl_compositor::*;
                match msg.opcode {
                    REQ_CREATE_SURFACE_OPCODE => {
                        let inner = proxy.lock().unwrap();
                        let data = Proxied::new(
                            None,
                            None,
                            handle,
                            wayland_server::protocol::wl_surface::WlSurface::interface(),
                        );
                        Some(data)
                    }
                    REQ_CREATE_REGION_OPCODE => todo!(),
                    _ => None,
                }
            }),
            Some(|proxy, handle, msg| None),
            &handle.backend_handle(),
            wayland_server::protocol::wl_compositor::WlCompositor::interface(),
        );
        let global = init.custom_init(global, data.clone());
        let proxy = state
            .c_registry
            .send_constructor::<I>(
                Request::Bind {
                    name: name.name,
                    id: (I::interface(), global.version()),
                },
                data.clone(),
            )
            .unwrap();
        let mut data = data.inner.lock().unwrap();
        data.client = proxy.id();
        data.server = global.id();
        data.version = global.version();
    }
}

impl GlobalDispatch<wls_seat::WlSeat, ProxiedGlobal> for State {
    fn bind(
        state: &mut State,
        handle: &wayland_server::DisplayHandle,
        _: &wayland_server::Client,
        global: New<wls_seat::WlSeat>,
        name: &ProxiedGlobal,
        init: &mut DataInit<'_, State>,
    ) {
        use wayland_client::protocol::wl_registry::Request;
        type I = wayland_client::protocol::wl_seat::WlSeat;
        let data = Proxied::new(
            Some(|proxy, handle, state, msg| {
                use wayland_server::protocol::wl_seat::*;
                dbg!(&msg);
                match msg.opcode {
                    _ => None,
                }
            }),
            Some(|proxy, handle, msg| {
                dbg!(msg);
                None
            }),
            &handle.backend_handle(),
            wayland_server::protocol::wl_seat::WlSeat::interface(),
        );
        let global = init.custom_init(global, data.clone());
        let proxy = state
            .c_registry
            .send_constructor::<I>(
                Request::Bind {
                    name: name.name,
                    id: (I::interface(), global.version()),
                },
                data.clone(),
            )
            .unwrap();
        let mut data = data.inner.lock().unwrap();
        data.client = proxy.id();
        data.server = global.id();
        data.version = global.version();

        // This is useless: dispatch_* will continue looping and will end up processing the
        // incoming sync() before the passed-along constructor even gets sent.
        state.dispatch_server = false;
        let _ = state.c_display.sync(&state.c_qh, Registry);
    }
}

async fn run(s_stream: UnixStream) -> Result<(), Box<dyn Error>> {
    let c_conn = Connection::connect_to_env()?;
    let c_display = c_conn.display();
    let mut c_queue = c_conn.new_event_queue();
    let c_qh = c_queue.handle();
    let c_backend = c_conn.backend();
    let mut c_reader = c_conn.prepare_read();
    let c_fd = c_reader.as_ref().unwrap().connection_fd().as_raw_fd();
    let c_fd = AsyncFd::new(Fd(c_fd))?;
    let c_registry = c_display.get_registry(&c_qh, Registry);
    let _ = c_display.sync(&c_qh, Registry);

    let mut s_display = Display::new()?;
    let mut s_handle = s_display.handle();
    let s_client = s_handle.insert_client(s_stream, Arc::new(()))?;
    let s_fd = AsyncFd::new(Fd(s_display.as_fd().as_raw_fd()))?;
    let mut state = State {
        dispatch_server: false,
        c_display,
        c_qh,
        c_backend,
        c_registry,
        s_handle,
        s_client,
    };

    poll_fn(|cx| {
        loop {
            if c_reader.is_none() {
                c_reader = c_conn.prepare_read();
            }
            let c_read = c_fd.poll_read_ready(cx)?;
            if let task::Poll::Ready(mut g) = c_read {
                use wayland_client::backend::WaylandError;
                dbg!("client");
                match c_reader.take().unwrap().read() {
                    Ok(_) => continue,
                    Err(WaylandError::Io(e)) if e.kind() == io::ErrorKind::WouldBlock => {
                        g.clear_ready();
                    }
                    Err(e) => Err(e)?,
                }
            }
            break;
        }

        let _ = c_queue.poll_dispatch_pending(cx, &mut state)?;
        if state.dispatch_server {
            let s_ready = s_fd.poll_read_ready(cx)?;
            dbg!(s_ready.is_ready());
            s_display.dispatch_clients(&mut state)?;
            s_display.flush_clients()?;
            if let task::Poll::Ready(mut g) = s_ready {
                g.clear_ready();
            }
        }
        c_queue.flush()?;
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
