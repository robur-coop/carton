type file_descr = Unix.file_descr * int

val map : file_descr -> pos:int -> int -> Cachet.bigstring

val index :
     ?pagesize:int
  -> ?cachesize:int
  -> hash_length:int
  -> ref_length:int
  -> Fpath.t
  -> file_descr Classeur.t

val make :
     ?pagesize:int
  -> ?cachesize:int
  -> ?z:De.bigstring
  -> ref_length:int
  -> ?index:(Carton.Uid.t -> int)
  -> Fpath.t
  -> file_descr Carton.t

type entry = { offset: int; crc: Optint.t; consumed: int; size: int }

type config = {
    threads: int option
  ; pagesize: int option
  ; cachesize: int option
  ; ref_length: int
  ; identify: Carton.identify
  ; on_entry: max:int -> entry -> unit
  ; on_object: Carton.Value.t -> Carton.Uid.t -> unit
}

val config :
     ?threads:int
  -> ?pagesize:int
  -> ?cachesize:int
  -> ?on_entry:(max:int -> entry -> unit)
  -> ?on_object:(Carton.Value.t -> Carton.Uid.t -> unit)
  -> ref_length:int
  -> Carton.identify
  -> config

val verify_from_pack :
     cfg:config
  -> digest:Carton.First_pass.digest
  -> Fpath.t
  -> Carton.status array * string

val verify_from_idx :
     cfg:config
  -> digest:Carton.First_pass.digest
  -> Fpath.t
  -> Carton.status array * string

module Window : sig
  type 'meta t

  val make : unit -> 'meta t
  val is_full : 'meta t -> bool
end

val delta :
     ref_length:int
  -> load:(Carton.Uid.t -> 'meta -> Carton.Value.t)
  -> 'meta Cartonnage.Entry.t Seq.t
  -> 'meta Cartonnage.Target.t Seq.t

val to_pack :
     ?with_header:int
  -> ?with_signature:Carton.First_pass.digest
  -> ?cursor:int
  -> ?level:int
  -> load:(Carton.Uid.t -> 'meta -> Carton.Value.t)
  -> 'meta Cartonnage.Target.t Seq.t
  -> string Seq.t

type delta = { source: Carton.Uid.t; depth: int; raw: Cachet.Bstr.t }

val entries_of_pack :
     cfg:config
  -> digest:Carton.First_pass.digest
  -> Fpath.t
  -> (file_descr Carton.t * delta option) Cartonnage.Entry.t array

val delta_from_pack :
     ref_length:int
  -> windows:('fd Carton.t * delta option) Window.t array
  -> ('fd Carton.t * delta option) Cartonnage.Entry.t Seq.t
  -> ('fd Carton.t * delta option) Cartonnage.Target.t Seq.t

type sort = {
    sort: 'a. 'a Cartonnage.Entry.t array list -> 'a Cartonnage.Entry.t Seq.t
}
[@@unboxed]

val merge :
     cfg:config
  -> digest:Carton.First_pass.digest
  -> sort:sort
  -> ?level:int
  -> Fpath.t list
  -> string Seq.t
