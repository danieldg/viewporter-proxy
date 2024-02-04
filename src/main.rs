use std::{
    collections::HashMap,
    error::Error,
    ffi::CString,
    future::{poll_fn, Future},
    io,
    os::fd::{AsFd, AsRawFd, OwnedFd},
    os::unix::net::UnixStream,
    pin::pin,
    sync::{
        atomic::{AtomicU32, Ordering},
        Arc, Mutex, Weak,
    },
    task,
};
use tokio::io::unix::AsyncFd;
use wayland_client::protocol::{
    wl_compositor, wl_data_device, wl_data_device_manager, wl_display, wl_output, wl_pointer,
    wl_registry, wl_seat, wl_shm, wl_subcompositor, wl_subsurface, wl_surface, wl_touch,
};
use wayland_client::{Connection, Dispatch, Proxy};
use wayland_protocols::{
    wp::{
        cursor_shape::v1::client::wp_cursor_shape_manager_v1,
        fractional_scale::v1::client::wp_fractional_scale_manager_v1,
        linux_dmabuf::zv1::client::{zwp_linux_buffer_params_v1, zwp_linux_dmabuf_v1},
        presentation_time::client::wp_presentation,
        primary_selection::zv1::client::zwp_primary_selection_device_manager_v1,
        single_pixel_buffer::v1::client::wp_single_pixel_buffer_manager_v1,
        text_input::zv3::client::zwp_text_input_manager_v3,
        viewporter::client::{wp_viewport, wp_viewporter},
    },
    xdg::activation::v1::client::xdg_activation_v1,
    xdg::decoration::zv1::client::zxdg_decoration_manager_v1,
    xdg::shell::client::{xdg_popup, xdg_positioner, xdg_surface, xdg_toplevel, xdg_wm_base},
};

fn scale_req_i32(x: i32) -> i32 {
    if x > i32::MAX / 3 {
        x
    } else {
        x * 3 / 2
    }
}

fn scale_req_arg<I, F>(arg: &mut wayland_client::backend::protocol::Argument<I, F>) {
    use wayland_client::backend::protocol::Argument as Arg;
    match arg {
        Arg::Int(x) => *x = scale_req_i32(*x),
        Arg::Uint(x) => *x = (*x * 3) / 2,
        Arg::Fixed(x) => *x = (*x * 3) / 2,
        _ => panic!(),
    }
}

fn scale_evt_arg<I, F>(arg: &mut wayland_client::backend::protocol::Argument<I, F>) {
    use wayland_client::backend::protocol::Argument as Arg;
    match arg {
        Arg::Int(i) => *i = (*i * 2 + 1) / 3,
        Arg::Uint(i) => *i = (*i * 2 + 1) / 3,
        Arg::Fixed(i) => *i = (*i * 2 + 1) / 3,
        _ => unreachable!(),
    }
}

fn lookup_interface(name: &[u8]) -> Option<&'static wayland_client::backend::protocol::Interface> {
    Some(match name {
        //  b"ext_idle_notifier_v1"
        //  b"org_kde_kwin_server_decoration_manager"
        b"wl_compositor" => wl_compositor::WlCompositor::interface(),
        b"wl_data_device_manager" => wl_data_device_manager::WlDataDeviceManager::interface(),
        b"wl_output" => wl_output::WlOutput::interface(),
        b"wl_seat" => wl_seat::WlSeat::interface(),
        b"wl_shm" => wl_shm::WlShm::interface(),
        b"wl_subcompositor" => wl_subcompositor::WlSubcompositor::interface(),
        //  b"wp_content_type_manager_v1"
        b"wp_cursor_shape_manager_v1" => {
            wp_cursor_shape_manager_v1::WpCursorShapeManagerV1::interface()
        }
        b"wp_fractional_scale_manager_v1" => {
            wp_fractional_scale_manager_v1::WpFractionalScaleManagerV1::interface()
        }
        b"wp_presentation" => wp_presentation::WpPresentation::interface(),
        b"wp_single_pixel_buffer_manager_v1" => {
            wp_single_pixel_buffer_manager_v1::WpSinglePixelBufferManagerV1::interface()
        }
        b"wp_viewporter" => wp_viewporter::WpViewporter::interface(),
        b"xdg_activation_v1" => xdg_activation_v1::XdgActivationV1::interface(),
        b"xdg_wm_base" => xdg_wm_base::XdgWmBase::interface(),
        //  b"zwp_idle_inhibit_manager_v1"
        b"zwp_linux_dmabuf_v1" => zwp_linux_dmabuf_v1::ZwpLinuxDmabufV1::interface(),
        //  b"zwp_pointer_constraints_v1" - set_cursor_position_hint
        //  b"zwp_pointer_gestures_v1"
        b"zwp_primary_selection_device_manager_v1" => {
            zwp_primary_selection_device_manager_v1::ZwpPrimarySelectionDeviceManagerV1::interface()
        }
        //  b"zwp_relative_pointer_manager_v1" - motion events
        //  b"zwp_tablet_manager_v2" - scale tool positions
        b"zwp_text_input_manager_v3" => {
            zwp_text_input_manager_v3::ZwpTextInputManagerV3::interface()
            // TODO set_cursor_rectangle
        }
        b"zxdg_decoration_manager_v1" => {
            zxdg_decoration_manager_v1::ZxdgDecorationManagerV1::interface()
        }
        //  b"zxdg_exporter_v1"
        //  b"zxdg_exporter_v2"
        //  b"zxdg_importer_v1"
        //  b"zxdg_importer_v2"
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
    backend: wayland_client::backend::Backend,
    state: Mutex<State>,
}

#[derive(Debug)]
struct State {
    shared: Weak<Shared>,
    display: wl_display::WlDisplay,
    backend: wayland_client::backend::Backend,
    globals: wayland_client::globals::GlobalList,
    viewporter: Option<Arc<Object>>,
    registries: Vec<Arc<Object>>,
    sids: HashMap<u32, Arc<Object>>,
    cids: HashMap<wayland_client::backend::ObjectId, Arc<Object>>,
    surfaces: HashMap<wayland_client::backend::ObjectId, SurfaceData>,
    sizes: HashMap<wayland_client::backend::ObjectId, (i32, i32)>,
}

#[derive(Debug)]
struct SurfaceData {
    scale: i32,
    viewport: Arc<Object>,
}

#[derive(Debug)]
struct Object {
    shared: Weak<Shared>,
    client: Mutex<wayland_client::backend::ObjectId>,
    server: AtomicU32,
    hook: AtomicU32,
    iface: &'static wayland_client::backend::protocol::Interface,
}

impl Object {
    fn new<I: Proxy>(shared: &Arc<Shared>, proxy: &I, server: u32) -> Arc<Self> {
        Arc::new(Object {
            shared: Arc::downgrade(shared),
            client: Mutex::new(proxy.id()),
            server: AtomicU32::new(server),
            hook: AtomicU32::new(HOOK_NONE),
            iface: I::interface(),
        })
    }

    fn new_hidden(
        shared: &Arc<Shared>,
        iface: &'static wayland_client::backend::protocol::Interface,
    ) -> Arc<Self> {
        Self::new_request_created(shared, 0, iface)
    }

    fn new_request_bind(
        shared: &Arc<Shared>,
        server: u32,
        iface: &'static wayland_client::backend::protocol::Interface,
    ) -> Arc<Self> {
        let rv = Self::new_request_created(shared, server, iface);
        let hook = match iface.name {
            "wl_compositor" => HOOK_COMPOSITOR,
            "wl_data_device_manager" => HOOK_DATA_DEVICE_MGR,
            "wl_output" => HOOK_OUTPUT,
            "wl_seat" => HOOK_SEAT,
            "wl_shm" => HOOK_SHM_GLOBAL,
            "wl_subcompositor" => HOOK_SUBCOMPOSITOR,
            "wp_fractional_scale_manager_v1" => HOOK_FSM,
            "wp_viewporter" => HOOK_VIEWPORTER,
            "xdg_wm_base" => HOOK_XDG_WM,
            "zwp_linux_dmabuf_v1" => HOOK_DMABUF_GLOBAL,
            _ => return rv,
        };
        rv.hook.store(hook, Ordering::Relaxed);
        rv
    }

    fn new_request_created(
        shared: &Arc<Shared>,
        server: u32,
        iface: &'static wayland_client::backend::protocol::Interface,
    ) -> Arc<Self> {
        Arc::new(Object {
            shared: Arc::downgrade(shared),
            client: Mutex::new(wayland_client::backend::ObjectId::null()),
            server: AtomicU32::new(server),
            hook: AtomicU32::new(HOOK_NONE),
            iface,
        })
    }

    fn new_event_created(
        shared: &Arc<Shared>,
        id: wayland_client::backend::ObjectId,
        server: u32,
    ) -> Arc<Self> {
        Arc::new(Object {
            shared: Arc::downgrade(shared),
            server: AtomicU32::new(server),
            hook: AtomicU32::new(HOOK_NONE),
            iface: id.interface(),
            client: Mutex::new(id),
        })
    }

    fn client(&self) -> wayland_client::backend::ObjectId {
        self.client.lock().unwrap().clone()
    }

    fn server(&self) -> u32 {
        self.server.load(Ordering::Relaxed)
    }

    fn intercept_request(
        &self,
        state: &mut State,
        msg: &mut wayland_client::backend::protocol::Message<
            wayland_client::backend::ObjectId,
            i32,
        >,
        newid: &mut Option<Arc<Object>>,
    ) -> (bool, Option<(Arc<Object>, Option<(i32, i32)>)>) {
        use wayland_client::backend::protocol::Argument as Arg;
        fn hook_newid(newid: &mut Option<Arc<Object>>, hook: u32) {
            if let Some(id) = newid {
                id.hook.store(hook, Ordering::Relaxed);
            }
        }

        match self.hook.load(Ordering::Relaxed) {
            HOOK_NONE => {}
            HOOK_REGISTRY => {}
            HOOK_COMPOSITOR => match msg.opcode {
                wl_compositor::REQ_CREATE_SURFACE_OPCODE => {
                    hook_newid(newid, HOOK_SURFACE);
                    return (true, newid.clone().map(|s| (s, None)));
                }
                wl_compositor::REQ_CREATE_REGION_OPCODE => hook_newid(newid, HOOK_REGION),
                _ => {}
            },
            HOOK_SURFACE => match msg.opcode {
                wl_surface::REQ_ATTACH_OPCODE => {
                    let sd = state.surfaces.get(&self.client()).unwrap();
                    let viewport = sd.viewport.client();
                    let scale = sd.scale;
                    let vp_size = state.sizes.get(&viewport);

                    if !vp_size.is_some_and(|&(x, _)| x >= 0) {
                        // buffer size changes don't matter if the viewport is being used
                        let size = match &msg.args[0] {
                            Arg::Object(buffer) => state.sizes.get(buffer),
                            _ => panic!(),
                        };
                        // buffer size is reduced by the declared scale
                        let (x, y) = size.cloned().map_or((-1, -1), |(x, y)| {
                            (scale_req_i32(x / scale), scale_req_i32(y / scale))
                        });
                        state
                            .backend
                            .send_request(
                                wayland_client::backend::protocol::Message {
                                    sender_id: viewport,
                                    opcode: wp_viewport::REQ_SET_DESTINATION_OPCODE,
                                    args: [Arg::Int(x), Arg::Int(y)].into_iter().collect(),
                                },
                                None,
                                None,
                            )
                            .unwrap();
                    }
                }
                wl_surface::REQ_SET_BUFFER_SCALE_OPCODE => {
                    let scale = match &msg.args[0] {
                        Arg::Int(i) => *i,
                        _ => panic!(),
                    };
                    state.surfaces.get_mut(&self.client()).unwrap().scale = scale;
                }
                _ => {}
            },
            HOOK_REGION => {
                for arg in &mut msg.args {
                    scale_req_arg(arg);
                }
            }
            HOOK_SUBCOMPOSITOR => hook_newid(newid, HOOK_SUBSURFACE),
            HOOK_SUBSURFACE => {
                if msg.opcode == wl_subsurface::REQ_SET_POSITION_OPCODE {
                    scale_req_arg(&mut msg.args[0]);
                    scale_req_arg(&mut msg.args[1]);
                }
            }
            HOOK_VIEWPORTER => {
                if msg.opcode == wp_viewporter::REQ_GET_VIEWPORT_OPCODE {
                    let surface = match &msg.args[1] {
                        Arg::Object(s) => state.surfaces.get(&s).unwrap(),
                        _ => panic!(),
                    };
                    let new_sid = newid.take().unwrap().server();
                    surface.viewport.server.store(new_sid, Ordering::Relaxed);
                    *newid = Some(surface.viewport.clone());
                    return (false, None);
                }
            }
            HOOK_VIEWPORT => match msg.opcode {
                wp_viewport::REQ_DESTROY_OPCODE => {
                    self.server.store(0, Ordering::Relaxed);

                    msg.opcode = wp_viewport::REQ_SET_SOURCE_OPCODE;
                    msg.args = [
                        Arg::Fixed(-256),
                        Arg::Fixed(-256),
                        Arg::Fixed(-256),
                        Arg::Fixed(-256),
                    ]
                    .into_iter()
                    .collect();

                    state.sizes.remove(&self.client());
                    return (true, None);
                }
                wp_viewport::REQ_SET_DESTINATION_OPCODE => {
                    let args = msg.args.split_first_mut().unwrap();
                    let size = match (args.0, &mut args.1[0]) {
                        (Arg::Int(x), Arg::Int(y)) => {
                            if *x > 0 {
                                *x = scale_req_i32(*x);
                                *y = scale_req_i32(*y);
                            }
                            (*x, *y)
                        }
                        _ => panic!(),
                    };
                    state.sizes.insert(self.client(), size);
                }
                _ => {}
            },
            HOOK_FSM => hook_newid(newid, HOOK_FSCALE),
            HOOK_FSCALE => {}
            HOOK_SHM_GLOBAL => hook_newid(newid, HOOK_SHM_POOL),
            HOOK_SHM_POOL => {
                if let Some(id) = newid {
                    id.hook.store(HOOK_BUFFER, Ordering::Relaxed);
                    let size = match (&msg.args[2], &msg.args[3]) {
                        (Arg::Int(x), Arg::Int(y)) => (*x, *y),
                        _ => panic!(),
                    };
                    return (true, Some((id.clone(), Some(size))));
                }
            }
            HOOK_BUFFER => {}
            HOOK_XDG_WM => match msg.opcode {
                xdg_wm_base::REQ_GET_XDG_SURFACE_OPCODE => hook_newid(newid, HOOK_XDG_SURFACE),
                xdg_wm_base::REQ_CREATE_POSITIONER_OPCODE => hook_newid(newid, HOOK_XDG_POSITIONER),
                _ => {}
            },
            HOOK_XDG_SURFACE => match msg.opcode {
                xdg_surface::REQ_GET_TOPLEVEL_OPCODE => hook_newid(newid, HOOK_XDG_TOPLEVEL),
                xdg_surface::REQ_GET_POPUP_OPCODE => hook_newid(newid, HOOK_XDG_POPUP),
                xdg_surface::REQ_SET_WINDOW_GEOMETRY_OPCODE => {
                    for arg in &mut msg.args {
                        scale_req_arg(arg);
                    }
                }
                _ => {}
            },
            HOOK_XDG_POSITIONER => match msg.opcode {
                xdg_positioner::REQ_SET_SIZE_OPCODE
                | xdg_positioner::REQ_SET_ANCHOR_RECT_OPCODE
                | xdg_positioner::REQ_SET_OFFSET_OPCODE
                | xdg_positioner::REQ_SET_PARENT_SIZE_OPCODE => {
                    for arg in &mut msg.args {
                        scale_req_arg(arg);
                    }
                }
                _ => {}
            },
            HOOK_XDG_TOPLEVEL => {}
            HOOK_XDG_POPUP => {}
            HOOK_DMABUF_GLOBAL => {
                if msg.opcode == zwp_linux_dmabuf_v1::REQ_CREATE_PARAMS_OPCODE {
                    hook_newid(newid, HOOK_DMABUF_PARAMS);
                }
            }
            HOOK_DMABUF_PARAMS => {
                if msg.opcode == zwp_linux_buffer_params_v1::REQ_CREATE_IMMED_OPCODE {
                    if let Some(id) = newid {
                        id.hook.store(HOOK_BUFFER, Ordering::Relaxed);
                        let size = match (&msg.args[1], &msg.args[2]) {
                            (Arg::Int(x), Arg::Int(y)) => (*x, *y),
                            _ => panic!(),
                        };
                        return (true, Some((id.clone(), Some(size))));
                    }
                } else if msg.opcode == zwp_linux_buffer_params_v1::REQ_CREATE_OPCODE {
                    let size = match (&msg.args[0], &msg.args[1]) {
                        (Arg::Int(x), Arg::Int(y)) => (*x, *y),
                        _ => panic!(),
                    };
                    state.sizes.insert(self.client(), size);
                }
            }
            HOOK_SEAT => match msg.opcode {
                wl_seat::REQ_GET_POINTER_OPCODE => hook_newid(newid, HOOK_POINTER),
                wl_seat::REQ_GET_TOUCH_OPCODE => hook_newid(newid, HOOK_TOUCH),
                _ => {}
            },
            HOOK_OUTPUT => {}
            HOOK_POINTER => {}
            HOOK_TOUCH => {}
            HOOK_DATA_DEVICE_MGR => match msg.opcode {
                wl_data_device_manager::REQ_GET_DATA_DEVICE_OPCODE => {
                    hook_newid(newid, HOOK_DATA_DEVICE)
                }
                _ => {}
            },
            HOOK_DATA_DEVICE => {}
            _ => unreachable!(),
        }
        (true, None)
    }

    fn intercept_request_post(&self, state: &mut State, data: (Arc<Object>, Option<(i32, i32)>)) {
        use wayland_client::backend::protocol::Argument as Arg;

        match data {
            (surface, None) => {
                let surface_cid = surface.client();
                let viewport = Arc::new(Object {
                    shared: surface.shared.clone(),
                    server: AtomicU32::new(0),
                    hook: AtomicU32::new(HOOK_VIEWPORT),
                    iface: wp_viewport::WpViewport::interface(),
                    client: Mutex::new(wayland_client::backend::ObjectId::null()),
                });
                let viewport_cid = state
                    .backend
                    .send_request(
                        wayland_client::backend::protocol::Message {
                            sender_id: state.viewporter.as_ref().unwrap().client(),
                            opcode: wp_viewporter::REQ_GET_VIEWPORT_OPCODE,
                            args: [
                                Arg::NewId(wayland_client::backend::ObjectId::null()),
                                Arg::Object(surface_cid.clone()),
                            ]
                            .into_iter()
                            .collect(),
                        },
                        Some(viewport.clone()),
                        None,
                    )
                    .unwrap();

                *viewport.client.lock().unwrap() = viewport_cid;

                state
                    .surfaces
                    .insert(surface_cid, SurfaceData { scale: 1, viewport });
            }
            (buffer, Some((x, y))) => {
                state.sizes.insert(buffer.client(), (x, y));
            }
        }
    }
}

macro_rules! pseudo_enum {
    (enum $name:ident { $($x:ident),* $(,)? }) => {
        #[allow(non_camel_case_types)]
        enum Hooks {
            $($x,)*
        }
        $(
            const $x: u32 = Hooks::$x as u32;
        )*
    };
}

pseudo_enum! {
    enum Hooks {
        HOOK_NONE,
        HOOK_REGISTRY,
        HOOK_COMPOSITOR,
        HOOK_SURFACE,
        HOOK_REGION,
        HOOK_SUBCOMPOSITOR,
        HOOK_SUBSURFACE,
        HOOK_VIEWPORTER,
        HOOK_VIEWPORT,
        HOOK_SHM_GLOBAL,
        HOOK_SHM_POOL,
        HOOK_BUFFER,
        HOOK_XDG_WM,
        HOOK_XDG_POSITIONER,
        HOOK_XDG_SURFACE,
        HOOK_XDG_TOPLEVEL,
        HOOK_XDG_POPUP,
        HOOK_DMABUF_GLOBAL,
        HOOK_DMABUF_PARAMS,
        HOOK_SEAT,
        HOOK_POINTER,
        HOOK_TOUCH,
        HOOK_FSM,
        HOOK_FSCALE,
        HOOK_OUTPUT,
        HOOK_DATA_DEVICE_MGR,
        HOOK_DATA_DEVICE,
    }
}

impl wayland_client::backend::ObjectData for Object {
    fn event(
        self: Arc<Self>,
        _: &wayland_client::backend::Backend,
        mut msg: wayland_client::backend::protocol::Message<
            wayland_client::backend::ObjectId,
            OwnedFd,
        >,
    ) -> Option<Arc<(dyn wayland_client::backend::ObjectData + 'static)>> {
        let shared = self.shared.upgrade()?;
        let sid = self.server();
        match self.hook.load(Ordering::Relaxed) {
            HOOK_NONE => {}
            HOOK_SURFACE => {}
            HOOK_SHM_GLOBAL => {}
            HOOK_BUFFER => {}
            HOOK_XDG_WM => {}
            HOOK_XDG_SURFACE => {}
            HOOK_XDG_TOPLEVEL => match msg.opcode {
                xdg_toplevel::EVT_CONFIGURE_OPCODE | xdg_toplevel::EVT_CONFIGURE_BOUNDS_OPCODE => {
                    scale_evt_arg(&mut msg.args[0]);
                    scale_evt_arg(&mut msg.args[1]);
                }
                _ => {}
            },
            HOOK_XDG_POPUP => match msg.opcode {
                xdg_popup::EVT_CONFIGURE_OPCODE => {
                    scale_evt_arg(&mut msg.args[0]);
                    scale_evt_arg(&mut msg.args[1]);
                    scale_evt_arg(&mut msg.args[2]);
                    scale_evt_arg(&mut msg.args[3]);
                }
                _ => {}
            },
            HOOK_DMABUF_GLOBAL => {}
            HOOK_DMABUF_PARAMS => {}
            HOOK_OUTPUT => match msg.opcode {
                wl_output::EVT_SCALE_OPCODE => {
                    msg.args[0] = wayland_client::backend::protocol::Argument::Int(2);
                }
                _ => {}
            },
            HOOK_SEAT => {}
            HOOK_POINTER => match msg.opcode {
                wl_pointer::EVT_ENTER_OPCODE => {
                    scale_evt_arg(&mut msg.args[2]);
                    scale_evt_arg(&mut msg.args[3]);
                }
                wl_pointer::EVT_MOTION_OPCODE => {
                    scale_evt_arg(&mut msg.args[1]);
                    scale_evt_arg(&mut msg.args[2]);
                }
                _ => {}
            },
            HOOK_TOUCH => match msg.opcode {
                wl_touch::EVT_DOWN_OPCODE => {
                    scale_evt_arg(&mut msg.args[4]);
                    scale_evt_arg(&mut msg.args[5]);
                }
                wl_touch::EVT_MOTION_OPCODE => {
                    scale_evt_arg(&mut msg.args[2]);
                    scale_evt_arg(&mut msg.args[3]);
                }
                wl_touch::EVT_SHAPE_OPCODE => {
                    scale_evt_arg(&mut msg.args[1]);
                    scale_evt_arg(&mut msg.args[2]);
                }
                _ => {}
            },
            HOOK_FSCALE => {
                // This one is backwards: scale goes up as the multiplier increases
                scale_req_arg(&mut msg.args[0]);
            }
            HOOK_DATA_DEVICE => match msg.opcode {
                wl_data_device::EVT_ENTER_OPCODE => {
                    scale_req_arg(&mut msg.args[2]);
                    scale_req_arg(&mut msg.args[3]);
                }
                wl_data_device::EVT_MOTION_OPCODE => {
                    scale_req_arg(&mut msg.args[1]);
                    scale_req_arg(&mut msg.args[2]);
                }
                _ => {}
            },
            _ => unreachable!(),
        }

        let created = shared.server_out_raw(sid, msg.opcode, &msg.args);
        if let Some(created) = &created {
            if self.hook.load(Ordering::Relaxed) == HOOK_DMABUF_PARAMS
                && msg.opcode == zwp_linux_buffer_params_v1::EVT_CREATED_OPCODE
            {
                let shared = self.shared.upgrade().unwrap();
                let mut state = shared.state.lock().unwrap();
                let size = state.sizes.remove(&self.client()).unwrap();
                state.sizes.insert(created.client(), size);
            }
        }
        created.map(|x| x as _)
    }

    fn destroyed(&self, cid: wayland_client::backend::ObjectId) {
        use wayland_client::backend::protocol::Argument as Arg;
        let Some(shared) = self.shared.upgrade() else {
            return;
        };
        debug_assert_eq!(self.client(), cid);
        let sid = self.server();
        if sid > 0 && sid < 0xff000000 {
            shared.server_out_raw(1, wl_display::EVT_DELETE_ID_OPCODE, &[Arg::Uint(sid)]);
        }
        // Note: this function may be called from send_request if it sends a destructor
        let mut state = shared.state.lock().unwrap();
        state.sids.remove(&sid);
        state.cids.remove(&cid);
        state.surfaces.remove(&cid);
        state.sizes.remove(&cid);
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
        let Some(sender) = state.sids.get(&sender_sid).cloned() else {
            return;
        };
        let mut fd_count = 0;

        let mut msg = wayland_client::backend::protocol::Message {
            sender_id: sender.client(),
            opcode,
            args: Default::default(),
        };

        let desc = &sender.iface.requests[opcode as usize];

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
                        .map(|i| Object::new_request_created(self, new_sid, i));
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
            obj.hook.store(HOOK_REGISTRY, Ordering::Relaxed);
            state.registries.push(obj.clone());
            state.sids.insert(new_sid, obj);
            state.globals.contents().with_list(|list| {
                for global in list {
                    self.send_global(new_sid, global.name, &global.interface, global.version);
                }
            });
            return;
        }

        if new_sid != 0 && newid_data.is_none() {
            assert_eq!(sender.hook.load(Ordering::Relaxed), HOOK_REGISTRY);
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
            newid_data = Some(Object::new_request_bind(self, new_sid, iface));
        }

        let (send, post) = sender.intercept_request(&mut state, &mut msg, &mut newid_data);

        if let Some(obj) = &newid_data {
            state.sids.insert(new_sid, obj.clone());
        }

        if send {
            drop(state);
            // Don't hold the lock while calling send_request

            let oid = self
                .backend
                .send_request(msg, newid_data.clone().map(|x| x as _), child_spec)
                .unwrap();

            state = self.state.lock().unwrap();
            if let Some(obj) = newid_data {
                *obj.client.lock().unwrap() = oid.clone();
                state.cids.insert(oid, obj);
            }
        }

        if let Some(post) = post {
            sender.intercept_request_post(&mut state, post);
        }

        fds.drain(..fd_count);
    }

    fn send_global(self: &Arc<Self>, sid: u32, name: u32, interface: &str, version: u32) {
        use wayland_client::backend::protocol::Argument as Arg;
        let Some(iface) = lookup_interface(interface.as_bytes()) else {
            return;
        };
        let version = version.min(iface.version);
        self.server_out_raw(
            sid,
            wl_registry::EVT_GLOBAL_OPCODE,
            &[
                Arg::Uint(name),
                Arg::Str(Some(Box::new(CString::new(interface).unwrap()))),
                Arg::Uint(version),
            ],
        );
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
                    let sid = state.cids.get(cid).map_or(0, |obj| obj.server());
                    buf.put_u32_le(sid);
                }
                Arg::NewId(cid) => {
                    let sid = cid.protocol_id();
                    let mut state = self.state.lock().unwrap();
                    let obj = Object::new_event_created(self, cid.clone(), sid);
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

impl Dispatch<wl_registry::WlRegistry, wayland_client::globals::GlobalListContents> for State {
    fn event(
        state: &mut State,
        _: &wl_registry::WlRegistry,
        event: wl_registry::Event,
        _: &wayland_client::globals::GlobalListContents,
        _: &Connection,
        _: &wayland_client::QueueHandle<State>,
    ) {
        use wl_registry::Event;
        match event {
            Event::Global {
                name,
                interface,
                version,
            } => {
                for reg in &state.registries {
                    state.shared.upgrade().unwrap().send_global(
                        reg.server(),
                        name,
                        &interface,
                        version,
                    );
                }
            }
            Event::GlobalRemove { name } => {
                use wayland_client::backend::protocol::Argument as Arg;
                for reg in &state.registries {
                    state.shared.upgrade().unwrap().server_out_raw(
                        reg.server(),
                        wl_registry::EVT_GLOBAL_REMOVE_OPCODE,
                        &[Arg::Uint(name)],
                    );
                }
            }
            _ => unreachable!(),
        }
    }
}

async fn run(server: UnixStream) -> Result<(), Box<dyn Error>> {
    let server = AsyncFd::new(server)?;
    let conn = Connection::connect_to_env()?;
    let display = conn.display();
    let (globals, mut queue) = wayland_client::globals::registry_queue_init(&conn)?;
    let backend = conn.backend();
    let mut c_reader = conn.prepare_read();
    let c_fd = c_reader.as_ref().unwrap().connection_fd().as_raw_fd();
    let c_fd = AsyncFd::new(Fd(c_fd))?;

    let shared = Arc::new_cyclic(|me| Shared {
        server,
        backend: backend.clone(),
        state: Mutex::new(State {
            shared: me.clone(),
            sids: HashMap::new(),
            cids: HashMap::new(),
            surfaces: HashMap::new(),
            sizes: HashMap::new(),
            viewporter: None,
            display,
            backend,
            globals,
            registries: Vec::new(),
        }),
    });

    if let Ok(state) = shared.state.lock().as_deref_mut() {
        use wayland_client::backend::protocol::Argument as Arg;

        let obj = Object::new(&shared, &state.display, 1);
        state.sids.insert(1, obj.clone());
        state.cids.insert(state.display.id(), obj);

        state.globals.contents().with_list(|list| {
            for global in list {
                if global.interface == "wp_viewporter" {
                    let viewporter =
                        Object::new_hidden(&shared, wp_viewporter::WpViewporter::interface());
                    let viewporter_cid = shared
                        .backend
                        .send_request(
                            wayland_client::backend::protocol::Message {
                                sender_id: state.globals.registry().id(),
                                opcode: wl_registry::REQ_BIND_OPCODE,
                                args: [
                                    Arg::Uint(global.name),
                                    Arg::Str(Some(Box::new(
                                        CString::new("wp_viewporter").unwrap(),
                                    ))),
                                    Arg::Uint(global.version),
                                    Arg::NewId(wayland_client::backend::ObjectId::null()),
                                ]
                                .into_iter()
                                .collect(),
                            },
                            Some(viewporter.clone()),
                            Some((wp_viewporter::WpViewporter::interface(), global.version)),
                        )
                        .unwrap();
                    *viewporter.client.lock().unwrap() = viewporter_cid;
                    state.viewporter = Some(viewporter);
                }
            }
        });
    }

    let mut parse = pin!(shared.clone().server_parse());

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

        let _ = queue.poll_dispatch_pending(cx, &mut *shared.state.lock().unwrap())?;
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
