use std::{
    collections::HashMap,
    error::Error,
    ffi::CString,
    future::{poll_fn, Future},
    io,
    os::fd::{AsFd, AsRawFd, OwnedFd},
    os::unix::net::UnixStream,
    pin::pin,
    sync::{Arc, Mutex, Weak},
    task,
};
use tokio::io::unix::AsyncFd;
use wayland_client::protocol::{wl_display, wl_registry};
use wayland_client::{Connection, Dispatch, Proxy};

fn lookup_interface(name: &[u8]) -> Option<&'static wayland_client::backend::protocol::Interface> {
    Some(match name {
        //  b"ext_idle_notifier_v1"
        //  b"org_kde_kwin_server_decoration_manager"
        b"wl_compositor" => wayland_client::protocol::wl_compositor::WlCompositor::interface(),
        b"wl_data_device_manager" => wayland_client::protocol::wl_data_device_manager::WlDataDeviceManager::interface(),
        b"wl_output" => wayland_client::protocol::wl_output::WlOutput::interface(),
        b"wl_seat" => wayland_client::protocol::wl_seat::WlSeat::interface(),
        b"wl_shm" => wayland_client::protocol::wl_shm::WlShm::interface(),
        b"wl_subcompositor" => wayland_client::protocol::wl_subcompositor::WlSubcompositor::interface(),
        //  b"wp_content_type_manager_v1"
        //  b"wp_cursor_shape_manager_v1"
        //  b"wp_drm_lease_device_v1"
        b"wp_fractional_scale_manager_v1" => wayland_protocols::wp::fractional_scale::v1::client::wp_fractional_scale_manager_v1::WpFractionalScaleManagerV1::interface(),
        b"wp_presentation" => wayland_protocols::wp::presentation_time::client::wp_presentation::WpPresentation::interface(),
        //  b"wp_security_context_manager_v1"
        //  b"wp_single_pixel_buffer_manager_v1"
        b"wp_viewporter" => wayland_protocols::wp::viewporter::client::wp_viewporter::WpViewporter::interface(),
        //  b"xdg_activation_v1"
        b"xdg_wm_base" => wayland_protocols::xdg::shell::client::xdg_wm_base::XdgWmBase::interface(),
        //  b"zwp_idle_inhibit_manager_v1"
        //  b"zwp_linux_dmabuf_v1"
        //  b"zwp_pointer_constraints_v1"
        //  b"zwp_pointer_gestures_v1"
        //  b"zwp_primary_selection_device_manager_v1"
        //  b"zwp_relative_pointer_manager_v1"
        //  b"zwp_tablet_manager_v2"
        //  b"zwp_text_input_manager_v3"
        //  b"zxdg_decoration_manager_v1"
        //  b"zxdg_exporter_v1"
        //  b"zxdg_exporter_v2"
        //  b"zxdg_importer_v1"
        //  b"zxdg_importer_v2"
        //  b"zxdg_output_manager_v1"
        _ => return None,
    })
}

#[derive(Debug)]
struct Fd(i32);

impl AsRawFd for Fd {
    fn as_raw_fd(&self) -> i32 {
        self.0
    }
}

#[derive(Debug)]
struct Shared {
    server: AsyncFd<UnixStream>,
    state: Mutex<State>,
}

#[derive(Debug)]
struct State {
    display: wl_display::WlDisplay,
    backend: wayland_client::backend::Backend,
    globals: wayland_client::globals::GlobalList,
    registries: Vec<Arc<Object>>,
    sids: HashMap<u32, Arc<Object>>,
    cids: HashMap<wayland_client::backend::ObjectId, Arc<Object>>,
}

#[derive(Debug)]
struct Object {
    shared: Weak<Shared>,
    client: Mutex<wayland_client::backend::ObjectId>,
    server: u32,
    iface: &'static wayland_client::backend::protocol::Interface,
}

impl State {}

impl Object {
    fn new<I: Proxy>(shared: &Arc<Shared>, proxy: &I, server: u32) -> Arc<Self> {
        Arc::new(Object {
            shared: Arc::downgrade(shared),
            client: Mutex::new(proxy.id()),
            server,
            iface: I::interface(),
        })
    }

    fn new_id(
        shared: &Arc<Shared>,
        server: u32,
        iface: &'static wayland_client::backend::protocol::Interface,
    ) -> Arc<Self> {
        Arc::new(Object {
            shared: Arc::downgrade(shared),
            client: Mutex::new(wayland_client::backend::ObjectId::null()),
            server,
            iface,
        })
    }

    fn new_evc(
        shared: &Arc<Shared>,
        id: wayland_client::backend::ObjectId,
        server: u32,
    ) -> Arc<Self> {
        Arc::new(Object {
            shared: Arc::downgrade(shared),
            server,
            iface: id.interface(),
            client: Mutex::new(id),
        })
    }

    fn client(&self) -> wayland_client::backend::ObjectId {
        self.client.lock().unwrap().clone()
    }
}

impl Dispatch<wl_registry::WlRegistry, wayland_client::globals::GlobalListContents> for State {
    fn event(
        state: &mut State,
        _: &wl_registry::WlRegistry,
        event: wl_registry::Event,
        _: &wayland_client::globals::GlobalListContents,
        _: &Connection,
        _: &wayland_client::QueueHandle<State>,
    ) {
        todo!("{:?}", event);
    }
}

impl Shared {
    async fn server_parse(self: Arc<Self>) -> Result<(), Box<dyn Error>> {
        let mut buf = [0; 4096];
        let mut ad_buf = [0; rustix::cmsg_space!(ScmRights(32))];
        let mut b_start = 0;
        let mut b_end = 0;
        let mut fds = Vec::new();
        loop {
            if b_start == b_end {
                b_start = 0;
                b_end = 0;
            } else if b_end >= 4096 || b_start > 2000 {
                assert_ne!(b_start, 0);
                buf.copy_within(b_start..b_end, 0);
                b_end -= b_start;
                b_start = 0;
            }
            let mut rg = self.server.readable().await?;
            let mut ad = rustix::net::RecvAncillaryBuffer::new(&mut ad_buf);
            match rustix::net::recvmsg(
                rg.get_ref(),
                &mut [rustix::io::IoSliceMut::new(&mut buf[b_end..])],
                &mut ad,
                rustix::net::RecvFlags::CMSG_CLOEXEC,
            ) {
                Err(rustix::io::Errno::AGAIN) => {
                    rg.clear_ready();
                    continue;
                }
                r => {
                    let len = r?.bytes;
                    b_end += len;
                    if len == 0 {
                        return Ok(());
                    }
                }
            }
            for msg in ad.drain() {
                if let rustix::net::RecvAncillaryMessage::ScmRights(list) = msg {
                    fds.extend(list);
                }
            }
            loop {
                if b_end - b_start < 8 {
                    break;
                }
                let msg_len = buf[b_start + 6] as usize + buf[b_start + 7] as usize * 256;
                assert!(msg_len <= 4096);
                if b_end - b_start < msg_len {
                    break;
                }
                self.server_in(&buf[b_start..][..msg_len], &mut fds);
                b_start += msg_len;
            }
        }
    }

    fn server_in(self: &Arc<Self>, mut buf: &[u8], fds: &mut Vec<OwnedFd>) {
        use bytes::Buf;
        use wayland_client::backend::protocol::Argument as Arg;
        use wayland_client::backend::protocol::ArgumentType as ArgT;
        let sender_sid = buf.get_u32_le();
        let opcode = buf.get_u16_le();
        let _len = buf.get_u16_le();
        let mut state = self.state.lock().unwrap();
        let Some(object) = state.sids.get(&sender_sid) else {
            return;
        };
        let mut fd_count = 0;

        let mut msg = wayland_client::backend::protocol::Message {
            sender_id: object.client(),
            opcode,
            args: Default::default(),
        };

        let desc = &object.iface.requests[opcode as usize];

        let mut newid_data = None;
        let mut child_spec = None;
        let mut new_sid = 0;

        for arg in desc.signature {
            msg.args.push(match arg {
                ArgT::Int => Arg::Int(buf.get_i32_le()),
                ArgT::Uint => Arg::Uint(buf.get_u32_le()),
                ArgT::Fixed => Arg::Fixed(buf.get_i32_le()),
                ArgT::Str(_) => {
                    let len = buf.get_u32_le() as usize;
                    if len == 0 {
                        Arg::Str(None)
                    } else {
                        let vec = buf[..len].to_vec();
                        buf.advance((len + 3) & !3);
                        Arg::Str(CString::from_vec_with_nul(vec).map(Box::new).ok())
                    }
                }
                ArgT::Object(_) => {
                    let sid = buf.get_u32_le();
                    Arg::Object(
                        state
                            .sids
                            .get(&sid)
                            .map_or(wayland_client::backend::ObjectId::null(), |o| o.client()),
                    )
                }
                ArgT::NewId => {
                    new_sid = buf.get_u32_le();
                    newid_data = desc
                        .child_interface
                        .map(|i| Object::new_id(self, new_sid, i));
                    Arg::NewId(wayland_client::backend::ObjectId::null())
                }
                ArgT::Array => {
                    let len = buf.get_u32_le() as usize;
                    let vec = buf[..len].to_vec();
                    buf.advance((len + 3) & !3);
                    Arg::Array(Box::new(vec))
                }
                ArgT::Fd => {
                    let fd = fds[fd_count].as_raw_fd();
                    fd_count += 1;
                    Arg::Fd(fd)
                }
            });
        }

        if sender_sid == 1 && opcode == wl_display::REQ_GET_REGISTRY_OPCODE {
            let obj = Object::new(self, state.globals.registry(), new_sid);
            state.registries.push(obj.clone());
            state.sids.insert(new_sid, obj);
            state.globals.contents().with_list(|list| {
                for global in list {
                    if lookup_interface(global.interface.as_bytes()).is_none() {
                        continue;
                    }
                    self.server_out_raw(
                        new_sid,
                        wl_registry::EVT_GLOBAL_OPCODE,
                        &[
                            Arg::Uint(global.name),
                            Arg::Str(Some(Box::new(
                                CString::new(global.interface.clone()).unwrap(),
                            ))),
                            Arg::Uint(global.version),
                        ],
                    );
                }
            });
            return;
        }

        if new_sid != 0 && newid_data.is_none() {
            // TODO this just assumes it's registry bind
            // args: [numeric_name, interface_name, version, new_id]
            let iface = lookup_interface(match &msg.args[1] {
                Arg::Str(Some(s)) => dbg!(s).as_bytes(),
                _ => panic!(),
            })
            .unwrap();
            let version = match msg.args[2] {
                Arg::Uint(i) => i,
                _ => panic!(),
            };
            child_spec = Some((iface, version));
            newid_data = Some(Object::new_id(self, new_sid, iface));
        }

        if let Some(obj) = &newid_data {
            state.sids.insert(new_sid, obj.clone());
        }

        let oid = state
            .backend
            .send_request(msg, newid_data.clone().map(|x| x as _), child_spec)
            .unwrap();

        if let Some(obj) = newid_data {
            *obj.client.lock().unwrap() = oid.clone();
            state.cids.insert(oid, obj);
        }

        fds.drain(..fd_count);
    }

    fn server_out(
        self: &Arc<Self>,
        msg: &wayland_client::backend::protocol::Message<
            wayland_client::backend::ObjectId,
            OwnedFd,
        >,
    ) -> Option<Arc<Object>> {
        let lock = self.state.lock().unwrap();
        let obj = lock.cids.get(&msg.sender_id).unwrap();
        let sid = obj.server;
        drop(lock);
        self.server_out_raw(sid, msg.opcode, &msg.args)
    }

    fn server_out_raw(
        self: &Arc<Self>,
        sid: u32,
        opcode: u16,
        args: &[wayland_client::backend::protocol::Argument<
            wayland_client::backend::ObjectId,
            OwnedFd,
        >],
    ) -> Option<Arc<Object>> {
        use bytes::BufMut;
        use wayland_client::backend::protocol::Argument as Arg;
        let mut rv = None;
        let mut ad_buf = [0; rustix::cmsg_space!(ScmRights(8))];
        let mut ad = rustix::net::SendAncillaryBuffer::new(&mut ad_buf);
        let mut fds = Vec::new();
        let mut buf = Vec::with_capacity(128);
        buf.put_u32_le(sid);
        buf.put_u32_le(opcode as u32);
        for arg in args {
            match arg {
                Arg::Int(i) => buf.put_i32_le(*i),
                Arg::Uint(i) => buf.put_u32_le(*i),
                Arg::Fixed(i) => buf.put_i32_le(*i),
                Arg::Str(None) => buf.put_u32_le(0),
                Arg::Str(Some(s)) => {
                    let s = s.as_bytes_with_nul();
                    buf.put_u32_le(s.len() as _);
                    buf.extend_from_slice(s);
                    buf.resize((buf.len() + 3) & !3, 0);
                }
                Arg::Object(cid) => {
                    let state = self.state.lock().unwrap();
                    let sid = state.cids.get(cid).map_or(0, |obj| obj.server);
                    buf.put_u32_le(sid);
                }
                Arg::NewId(cid) => {
                    let sid = cid.protocol_id();
                    let mut state = self.state.lock().unwrap();
                    let obj = Object::new_evc(self, cid.clone(), sid);
                    state.cids.insert(cid.clone(), obj.clone());
                    state.sids.insert(sid, obj.clone());
                    rv = Some(obj);
                    buf.put_u32_le(sid);
                }
                Arg::Array(s) => {
                    buf.put_u32_le(s.len() as _);
                    buf.extend_from_slice(s);
                    buf.resize((buf.len() + 3) & !3, 0);
                }
                Arg::Fd(fd) => {
                    fds.push(fd.as_fd());
                }
            }
        }
        buf[6] = buf.len() as u8;
        buf[7] = (buf.len() >> 8) as u8;
        if !fds.is_empty() {
            ad.push(rustix::net::SendAncillaryMessage::ScmRights(&fds));
        }
        rustix::net::sendmsg(
            &self.server,
            &[rustix::io::IoSlice::new(&buf)],
            &mut ad,
            rustix::net::SendFlags::NOSIGNAL,
        )
        .unwrap();
        rv
    }
}

impl wayland_client::backend::ObjectData for Object {
    fn event(
        self: Arc<Self>,
        handle: &wayland_client::backend::Backend,
        msg: wayland_client::backend::protocol::Message<wayland_client::backend::ObjectId, OwnedFd>,
    ) -> Option<Arc<(dyn wayland_client::backend::ObjectData + 'static)>> {
        let shared = self.shared.upgrade()?;
        shared.server_out(&msg).map(|x| x as _)
    }
    fn destroyed(&self, _: wayland_client::backend::ObjectId) {}
}

async fn run(server: UnixStream) -> Result<(), Box<dyn Error>> {
    let conn = Connection::connect_to_env()?;
    let display = conn.display();
    let (globals, mut queue) = wayland_client::globals::registry_queue_init(&conn)?;
    let backend = conn.backend();
    let mut c_reader = conn.prepare_read();
    let c_fd = c_reader.as_ref().unwrap().connection_fd().as_raw_fd();
    let c_fd = AsyncFd::new(Fd(c_fd))?;

    let state = Arc::new(Shared {
        server: AsyncFd::new(server)?,
        state: Mutex::new(State {
            sids: HashMap::new(),
            cids: HashMap::new(),
            display,
            backend,
            globals,
            registries: Vec::new(),
        }),
    });

    if let Ok(lock) = state.state.lock().as_deref_mut() {
        let obj = Object::new(&state, &lock.display, 1);
        lock.sids.insert(1, obj.clone());
        lock.cids.insert(lock.display.id(), obj);
    }

    let mut parse = pin!(state.clone().server_parse());

    poll_fn(|cx| {
        loop {
            let c_read = c_fd.poll_read_ready(cx)?;
            if let task::Poll::Ready(mut g) = c_read {
                use wayland_client::backend::WaylandError;
                if c_reader.is_none() {
                    c_reader = conn.prepare_read();
                }
                match c_reader.take().unwrap().read() {
                    Ok(_) => continue,
                    Err(WaylandError::Io(e)) if e.kind() == io::ErrorKind::WouldBlock => {
                        g.clear_ready();
                    }
                    Err(e) => Err(e)?,
                }
            } else {
                break;
            }
        }

        match parse.as_mut().poll(cx)? {
            task::Poll::Ready(()) => return task::Poll::Ready(Ok(())),
            task::Poll::Pending => {}
        };

        let _ = queue.poll_dispatch_pending(cx, &mut *state.state.lock().unwrap())?;
        queue.flush()?;
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
