module M : sig
  type src = [ `Manual | `String of string ]
  type decode = [ `Await | `Header of int * int | `End | `Malformed of string ]
  type decoder

  val src : decoder -> Bstr.t -> int -> int -> unit
  val dst : decoder -> Bstr.t -> int -> int -> unit
  val source : decoder -> Bstr.t -> unit
  val src_rem : decoder -> int
  val dst_rem : decoder -> int
  val src_len : decoder -> int
  val dst_len : decoder -> int
  val decode : decoder -> decode
  val decoder : ?source:Bstr.t -> src -> decoder
end

module R : sig
  type src = [ `Manual | `String of string ]

  type decode =
    [ `Await
    | `Header of int * int
    | `Copy of int * int
    | `Insert of string
    | `End
    | `Malformed of string ]

  type decoder

  val src : decoder -> string -> int -> int -> unit
  val src_rem : decoder -> int
  val src_len : decoder -> int
  val dst_len : decoder -> int
  val decode : decoder -> decode
  val decoder : src -> decoder

  val of_seq :
       string Seq.t
    -> [ `Header of int * int | `Copy of int * int | `Insert of string ] Seq.t
end

module N : sig
  type dst = [ `Manual | `Buffer of Buffer.t ]
  type encode = [ `Await | `Copy of int * int | `Insert of string | `End ]
  type encoder

  val dst : encoder -> Bstr.t -> int -> int -> unit
  val dst_rem : encoder -> int
  val encoder : dst -> src_len:int -> dst_len:int -> encoder
  val encode : encoder -> encode -> [ `Ok | `Partial ]
end
