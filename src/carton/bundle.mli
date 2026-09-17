(** Decoder and encoder of a {i git-bundle} file.

    A bundle is a plain text header followed by a PACK stream:

    {v
    bundle       = signature *capability *prerequisite *reference LF pack
    signature    = "# v2 git bundle" LF / "# v3 git bundle" LF
    capability   = "@" key ["=" value] LF          ; v3 only
    prerequisite = "-" obj-id SP comment LF
    reference    = obj-id SP refname LF
    v}

    This module only deals with the header. *)

type version = [ `V2 | `V3 ]
(** The type of bundle versions. *)

type capability =
  [ `Object_format of string  (** ["sha1"] or ["sha256"]. *)
  | `Filter of string  (** A partial-clone filter specification. *)
  | `Unknown of string * string option  (** Any other [key]/[value]. *) ]
(** The type of capabilities (only for [`V3] bundles). *)

type t
(** The type of bundle headers. *)

val make :
     ?version:version
  -> ?ref_length:int
  -> ?capabilities:capability list
  -> ?prerequisites:(Carton.Uid.t * string option) list
  -> (string * Carton.Uid.t) list
  -> t
(** [make ?version ?ref_length ?capabilities ?prerequisites references] is the
    header advertising [references] (an association list from a full reference
    name such as ["refs/heads/main"] to the unique identifier it points to).

    @raise Invalid_argument if the header is invalid (see {!val:check}). *)

val version : t -> version
val capabilities : t -> capability list
val prerequisites : t -> (Carton.Uid.t * string option) list
val references : t -> (string * Carton.Uid.t) list

val ref_length : t -> int
(** [ref_length t] is the size (in bytes) of the unique identifiers used by [t].
    It can be given as such to {!val:Carton.First_pass.of_seq}. *)

val is_thin : t -> bool
(** [is_thin t] tells whether the PACK stream which follows [t] may be {i thin}.
*)

val pp : Format.formatter -> t -> unit
(** Pretty printer of {!type:t}. *)

(** {1 Decoding a bundle.} *)

type decoder
(** The type for decoders. *)

type src = [ `String of string | `Manual ]
(** The type for input sources. With a [`Manual] source the client must provide
    input with {!val:src}. *)

type decode =
  [ `Await of decoder
  | `Header of t * decoder
  | `Pack of string * decoder
  | `End
  | `Malformed of string ]
(** The type for decoding results. [`Header] is emitted exactly once, as soon as
    the empty line which terminates the header has been seen. Everything which
    follows is emitted verbatim as [`Pack] chunks until the end of the input. *)

val decoder : ?ref_length:int -> ?max_header_size:int -> src -> decoder
(** [decoder ?ref_length ?max_header_size src] is a decoder reading a bundle
    from [src].

    [ref_length] forces the size (in bytes) of the unique identifiers instead of
    deducing it from the [`Object_format] capability. [max_header_size] (which
    defaults to [0x100000]) bounds the number of bytes the header is allowed to
    take — it protects against a stream which would never send the empty line
    terminating the header. *)

val decode : decoder -> decode
val src : decoder -> Bstr.t -> int -> int -> decoder

val src_rem : decoder -> int
(** [src_rem decoder] returns how many byte(s) are not yet processed by the
    given [decoder]. *)

val of_seq :
     ?ref_length:int
  -> ?max_header_size:int
  -> string Seq.t
  -> [ `Header of t | `Pack of string ] Seq.t
(** [of_seq seq] analyses the bundle stream given by [seq]. The [`Header] value
    is yielded first, then the PACK stream chunk by chunk.

    @raise Failure if the bundle is malformed. *)

val split :
     ?ref_length:int
  -> ?max_header_size:int
  -> string Seq.t
  -> (t * string Seq.t, [> `Msg of string ]) result
(** [split seq] forces the header of the bundle stream [seq] and returns it
    along with the remaining PACK stream, which can then be given to
    {!val:Carton.First_pass.of_seq}.

    {b NOTE}: the returned sequence is {b not} persistent. *)

(** {1 Encoding a bundle.} *)

val check : t -> (unit, [> `Msg of string ]) result
(** [check t] verifies that [t] can be serialized. *)

val to_string : t -> string
(** [to_string t] is the serialization of the header [t], empty line included.

    @raise Invalid_argument if {!val:check} fails. *)

val to_seq : t -> string Seq.t -> string Seq.t
(** [to_seq t pack] is the bundle stream made of the header [t] followed by the
    PACK stream [pack].

    @raise Invalid_argument if {!val:check} fails. *)
