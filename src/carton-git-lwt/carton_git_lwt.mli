module Make (Hash : Digestif.S) : sig
  val config :
       ?threads:int
    -> ?on_entry:(max:int -> Carton_lwt.entry -> unit Lwt.t)
    -> ?on_object:(Carton.Value.t -> Carton.Uid.t -> unit Lwt.t)
    -> unit
    -> Carton_lwt.config

  val verify_from_stream :
       cfg:Carton_lwt.config
    -> append:(string -> off:int -> len:int -> unit Lwt.t)
    -> 'fd Cachet.t
    -> string Lwt_stream.t
    -> (Carton.status array * string) Lwt.t

  val delta :
       load:(Carton.Uid.t -> 'meta -> Carton.Value.t Lwt.t)
    -> 'meta Cartonnage.Entry.t Lwt_seq.t
    -> 'meta Cartonnage.Target.t Lwt_seq.t

  val to_pack :
       ?with_header:int
    -> ?with_signature:Hash.ctx
    -> ?cursor:int
    -> ?level:int
    -> load:(Carton.Uid.t -> 'meta -> Carton.Value.t Lwt.t)
    -> 'meta Cartonnage.Target.t Lwt_stream.t
    -> string Lwt_seq.t

  val index : length:int -> 'fd Cachet.t -> 'fd Classeur.t

  val make :
    ?z:Bstr.t -> ?index:(Carton.Uid.t -> int) -> 'fd Cachet.t -> 'fd Carton.t
end
