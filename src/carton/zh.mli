module N : sig
  type encoder
  type dst = [ `Buffer of Buffer.t | `Manual ]
  type ret = [ `Flush of encoder | `End ]

  val dst_rem : encoder -> int
  val dst : encoder -> Zl.bigstring -> int -> int -> encoder
  val encode : encoder -> ret

  val encoder :
       ?level:int
    -> i:Zl.bigstring
    -> q:De.Queue.t
    -> w:De.Lz77.window
    -> source:int
    -> Bstr.t
    -> dst
    -> Duff.hunk list
    -> encoder
end

module M : sig
  type decoder
  type src = [ `String of string | `Manual ]

  type decode =
    [ `Await of decoder
    | `Header of int * int * decoder
    | `End of decoder
    | `Malformed of string ]

  val src_len : decoder -> int
  val dst_len : decoder -> int
  val src_rem : decoder -> int
  val dst_rem : decoder -> int
  val src : decoder -> Zl.bigstring -> int -> int -> decoder
  val dst : decoder -> Bstr.t -> int -> int -> decoder
  val source : decoder -> Bstr.t -> decoder
  val decode : decoder -> decode

  val decoder :
       ?source:Bstr.t
    -> o:Zl.bigstring
    -> allocate:(int -> Zl.window)
    -> src
    -> decoder
end
