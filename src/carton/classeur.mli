type uid = private string
type 'fd t
type 'a fn = uid:uid -> crc:Optint.t -> offset:int -> 'a

val make :
     ?pagesize:int
  -> ?cachesize:int
  -> map:'fd Cachet.map
  -> 'fd
  -> length:int
  -> hash_length:int
  -> ref_length:int
  -> 'fd t

val of_cachet :
  length:int -> hash_length:int -> ref_length:int -> 'fd Cachet.t -> 'fd t

val copy : 'fd t -> 'fd t
val uid_of_string : 'fd t -> string -> (uid, [> `Msg of string ]) result
val uid_of_string_exn : 'fd t -> string -> uid
val unsafe_uid_of_string : string -> uid
val find_offset : 'fd t -> uid -> int
val find_crc : 'fd t -> uid -> Optint.t
val iter : fn:unit fn -> 'fd t -> unit
val map : fn:'a fn -> 'fd t -> 'a list
val exists : 'fd t -> uid -> bool
val max : 'fd t -> int
val get : 'fd t -> int -> uid * Optint.t * int
val get_uid : 'fd t -> int -> uid
val get_offset : 'fd t -> int -> int
val get_crc : 'fd t -> int -> Optint.t
val pack : 'fd t -> string
val idx : 'fd t -> string

module type UID = sig
  type t
  type ctx

  val empty : ctx
  val feed : ctx -> ?off:int -> ?len:int -> Bstr.t -> ctx
  val get : ctx -> t
  val compare : t -> t -> int
  val length : int
  val to_raw_string : t -> string
  val pp : Format.formatter -> t -> unit
end

module Encoder : sig
  type 'ctx hash = 'ctx Carton.First_pass.hash = {
      feed_bytes: bytes -> off:int -> len:int -> 'ctx -> 'ctx
    ; feed_bigstring: Bstr.t -> 'ctx -> 'ctx
    ; serialize: 'ctx -> string
    ; length: int
  }

  type digest = Carton.First_pass.digest = Digest : 'ctx hash * 'ctx -> digest
  type entry = { crc: Optint.t; offset: int64; uid: uid }
  type encoder
  type dst = [ `Channel of out_channel | `Buffer of Buffer.t | `Manual ]

  val encoder :
       dst
    -> digest:digest
    -> pack:string
    -> ref_length:int
    -> entry array
    -> encoder

  val encode : encoder -> [ `Await ] -> [ `Partial | `Ok ]
  val dst_rem : encoder -> int
  val dst : encoder -> bytes -> int -> int -> unit
end
