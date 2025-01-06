module Delta : sig
  type t = From of { level: int; src: Carton.Uid.t } | Zero

  val pp : Format.formatter -> t -> unit
end

module Entry : sig
  type 'meta t

  val make :
       kind:Carton.Kind.t
    -> length:int
    -> ?preferred:bool
    -> ?delta:Delta.t
    -> Carton.Uid.t
    -> 'meta
    -> 'meta t

  val length : 'meta t -> int
  val uid : 'meta t -> Carton.Uid.t
  val kind : 'meta t -> Carton.Kind.t
  val pp : Format.formatter -> 'meta t -> unit
  val meta : 'meta t -> 'meta
end

module Source : sig
  type 'meta t

  val depth : 'meta t -> int
  val uid : 'meta t -> Carton.Uid.t
  val length : 'meta t -> int
  val kind : 'meta t -> Carton.Kind.t
  val bigstring : 'meta t -> Cachet.bigstring
  val index : 'meta t -> Duff.index
end

module Patch : sig
  type t

  val of_delta :
       depth:int
    -> source:Carton.Uid.t
    -> src_len:int
    -> dst_len:int
    -> Duff.hunk list
    -> t

  val of_copy : depth:int -> source:Carton.Uid.t -> Cachet.bigstring -> t
  val source : t -> Carton.Uid.t
  val pp : Format.formatter -> t -> unit
  val length : t -> int
end

module Target : sig
  type 'meta t

  val make : ?patch:Patch.t -> 'meta Entry.t -> 'meta t
  val diff : 'meta t -> source:'src Source.t -> target:Carton.Value.t -> unit
  val kind : 'meta t -> Carton.Kind.t
  val uid : 'meta t -> Carton.Uid.t
  val length : 'meta t -> int
  val patch : 'meta t -> Patch.t option
  val depth : 'meta t -> int
  val to_source : 'meta t -> target:Carton.Value.t -> 'meta Source.t
  val meta : 'meta t -> 'meta
end

type buffers = {
    o: De.bigstring
  ; i: De.bigstring
  ; q: De.Queue.t
  ; w: De.Lz77.window
}

module Encoder : sig
  type encoder

  val encoder :
    ?level:int -> buffers:buffers -> 'meta Target.t -> Carton.Value.t -> encoder

  val encode : o:De.bigstring -> encoder -> [ `Flush of encoder * int | `End ]
  val dst : encoder -> De.bigstring -> int -> int -> encoder
end

type where = Carton.Uid.t -> int option

val encode :
     ?level:int
  -> buffers:buffers
  -> where:where
  -> 'meta Target.t
  -> target:Carton.Value.t
  -> cursor:int
  -> int * Encoder.encoder
