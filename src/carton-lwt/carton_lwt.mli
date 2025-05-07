type entry = { offset: int; crc: Optint.t; consumed: int; size: int }
type config

val config :
     ?threads:int
  -> ?on_entry:(max:int -> entry -> unit Lwt.t)
  -> ?on_object:(Carton.Value.t -> Carton.Uid.t -> unit Lwt.t)
  -> ref_length:int
  -> Carton.identify
  -> config

val verify_from_stream :
     cfg:config
  -> digest:Carton.First_pass.digest
  -> append:(string -> off:int -> len:int -> unit Lwt.t)
  -> 'fd Cachet.t
  -> string Lwt_stream.t
  -> (Carton.status array * string) Lwt.t

val delta :
     ref_length:int
  -> load:(Carton.Uid.t -> 'meta -> Carton.Value.t Lwt.t)
  -> 'meta Cartonnage.Entry.t Lwt_seq.t
  -> 'meta Cartonnage.Target.t Lwt_seq.t

val to_pack :
     ?with_header:int
  -> ?with_signature:Carton.First_pass.digest
  -> ?cursor:int
  -> ?level:int
  -> load:(Carton.Uid.t -> 'meta -> Carton.Value.t Lwt.t)
  -> 'meta Cartonnage.Target.t Lwt_stream.t
  -> string Lwt_seq.t

val index :
     length:int
  -> hash_length:int
  -> ref_length:int
  -> 'fd Cachet.t
  -> 'fd Classeur.t

val make :
     ?z:Bstr.t
  -> ref_length:int
  -> ?index:(Carton.Uid.t -> int)
  -> 'fd Cachet.t
  -> 'fd Carton.t
