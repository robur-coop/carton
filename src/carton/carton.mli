(** Decoder of a PACK file.

    This module implements what is needed to decode a PACKv2 file. It is
    independent of any scheduler. For cooperation issues, we recommend that you
    refer to the documentation for Cachet, the library used to read a PACK file.
    More specifically, Carton is based on the use of [Unix.map_file] (or
    equivalent). Access to a block-device or file does {b not} block, but it can
    take time. In short, the cooperation points have to be added by the user. As
    an {i atomic} operation such as reading the PACK file (via [Unix.map_file])
    cannot be interleaved by cooperation points.

    The module is divided into 3 parts:
    - the first consists of a {!module:First_pass} module for analysing a PACK
      stream. This is useful when the PACK file is transmitted over the network.
      In such a case, this analysis can be applied.
    - the second consists of extracting objects from a PACK file, the contents
      of which can be made available via a [Unix.map_file] function.
    - the third consists of a few types that may be useful for checking a PACK
      file. Documentation is provided to explain how to use these types. *)

module H = H
module Zh = Zh

val bigstring_of_string : string -> Cachet.bigstring

module Kind : sig
  (** A PACK file contains several types of object. According to Git, it
      contains commits ([`A]), trees ([`B]), blobs ([`C]) and tags ([`D]).
      Carton is far enough removed from Git to abstract itself from the actual
      type of these objects. *)

  type t = [ `A | `B | `C | `D ]
  (** The type of kinds of objects. *)

  val pp : Format.formatter -> t -> unit
  val compare : t -> t -> int
  val equal : t -> t -> bool
  val to_int : t -> int
end

module Size : sig
  (** The size is a non-negative number which corresponds to the size of a
      {!module:Blob} in memory in bytes.

      {b Note}: We're not talking about the size of an object in the PACK file,
      but the size of the buffers needed to extract an object. Indeed, depending
      on the object and the way it is serialized in the PACK file, it may be
      necessary for the size to be larger than the object itself, particularly
      to store the source needed to build a patched object. *)

  type t = private int

  val of_int_exn : int -> t
  val to_int : t -> int
  val zero : t
  val compare : t -> t -> int
  val equal : t -> t -> bool
  val max : t -> t -> t
end

module Uid : sig
  type t = private string

  val unsafe_of_string : string -> t
  val pp : Format.formatter -> t -> unit
  val compare : t -> t -> int
  val equal : t -> t -> bool
end

(** {1 First-pass of a PACK file.}

    When manipulating a PACK file, it may be necessary to aggregate certain
    information before extracting objects (such as the size of the buffers that
    need to be allocated to extract objects). Analysis of the PACK file in the
    form of a {i stream} is therefore possible and is implemented in this
    {!module:First_pass} module.

    More concretely, when a user uploads the state of a Git repository
    ([git fetch]), a PACK file is transmitted. This analysis can be applied when
    the PACK file is received (at the same time) and the state can then be saved
    in the [.git] folder.

    {[
      $ git clone ...
      remote: Enumerating objects: 105, done.
      remote: Counting objects: 100% (105/105), done.
      remote: Compressing objects: 100% (81/81), done.
      remote: Total 305 (delta 41), reused 75 (delta 23), pack-reused 200
      Receiving objects: 100% (305/305), 100.00 KiB | 0 bytes/s, done. <-- first pass
    ]}

    The advantage of this module is that it can aggregate information at the
    same time as receiving the PACK file from the network. *)
module First_pass : sig
  type 'ctx hash = {
      feed_bytes: bytes -> off:int -> len:int -> 'ctx -> 'ctx
    ; feed_bigstring: De.bigstring -> 'ctx -> 'ctx
    ; serialize: 'ctx -> string
    ; length: int
  }
  (** A PACK file is always associated with a signature that verifies the
      integrity of the entire PACK file. As far as Git is concerned, this
      signature uses the SHA1 hash algorithm. Carton allows another algorithm to
      be used if required, provided that the user gives it a digest value
      allowing verification of the integrity of the PACK file segment by
      segment.

      {[
        $ head -c 20 pack-2d9d562730d25620c12799c0bf0d5baf9fd00896.pack|sha1sum
        2d9d562730d25620c12799c0bf0d5baf9fd00896  -
        $ xxd -p -l 20 -seek -20 pack-2d9d562730d25620c12799c0bf0d5baf9fd00896.pack
        2d9d562730d25620c12799c0bf0d5baf9fd00896
      ]}

      Here's an example of how to propose an algorithm to Carton with Digestif:

      {[
        let sha1 =
          let open Digestif.SHA1 in
          let feed bstr ctx = feed_bigstring ctx bstr in
          { feed; serialize= get; length= digest_size }

        let sha1 = Digest (sha1, Digestif.SHA1.empty)
      ]} *)

  type digest = Digest : 'ctx hash * 'ctx -> digest

  (** Type of PACK entries.

      Entries in a PACK file can be:
      - a compressed object as a base with its type
      - an object that can be built using another object (which may ultimately
        be a base or another object that needs another source)

      For the second category, the source can be found via a cursor
      ([OBJ_OFS_DELTA]) or a unique identifier ([OBJ_REF_DELTA]).

      An {!const:Ofs} type entry is a patch that requires a source to be built.
      This source is available upstream of the entry, and its position can be
      calculated using the current position of the entry minus the [sub] value.
      The patch gives information about the actual size of the object [target]
      and the expected size of the source.

      A {!const:Ref} type entry is a patch that also needs a source to build
      itself. The patch informs you of the actual size of the object [target]
      and the size of the expected source. The source can be found thanks to the
      [ptr] value given, which corresponds to the unique identifier of the
      source object (as far as Git is concerned, this identifier corresponds to
      what [git hash-object] can give). *)
  type kind =
    | Base of Kind.t
    | Ofs of { sub: int; source: Size.t; target: Size.t }
    | Ref of { ptr: Uid.t; source: Size.t; target: Size.t }

  (** {2 A delta-object, an object which requires a source.}

      As explained {{!type:kind} above}, entries can be a simple compression of
      the object or a "patch" requiring the source to be an object. The entry in
      a PACK file can refer to its source using {{!const:Ofs} its position} or a
      unique {{!const:Ref} identifier}.

      The case where an entry depends on a reference only arises for {i thin}
      PACK files. These sources are often available elsewhere than in the PACK
      file. A registered PACK file should {b not} contain references, but it is
      possible to transmit a PACK file with references to objects existing in
      other PACK files. One step in recording a PACK file is to {i canonicalise}
      it: in other words, to ensure that the PACK file is sufficient in itself
      to extract all the objects.

      The first pass is useful for identifying whether a PACK file is {i thin}
      or not. Objects are not extracted but identified. It is then up to the
      user to decide whether or not to canonicalise the PACK file. *)

  type entry = {
      offset: int  (** Absolute offset into the given PACK file. *)
    ; kind: kind  (** Kind of the object. *)
    ; size: Size.t  (** Length of the inflated object. *)
    ; consumed: int
          (** Length of the deflated object (as it is into the PACK file). *)
    ; crc: Optint.t
          (** Check-sum of the entry (header plus the deflated object). *)
  }
  (** Type of a PACK entries.

      {b Note}: The size given by the input is not necessarily the actual size
      of the object. It is when the object is a {!const:Base}. However, if the
      object is a patch ({!const:Ofs} or {!const:Ref}), the size of the patch is
      given. {!const:Ofs} and {!const:Ref} give the real size of the object via
      the [target] field. *)

  type decoder
  (** The type for decoders. *)

  type src = [ `Channel of in_channel | `String of string | `Manual ]
  (** The type for input sources. With a [`Manual] source the client must
      provide input with {!src}. *)

  type decode =
    [ `Await of decoder
    | `Peek of decoder
    | `Entry of entry * decoder
    | `End of string
    | `Malformed of string ]

  val decoder :
       output:De.bigstring
    -> allocate:(int -> De.window)
    -> ref_length:int
    -> digest:digest
    -> src
    -> decoder

  val decode : decoder -> decode

  val number_of_objects : decoder -> int
  (** [number_of_objects decoder] returns the number of objects available into
      the PACK file. *)

  val version : decoder -> int
  (** [version decoder] return the version of the PACK file (should be [2]). *)

  val counter_of_objects : decoder -> int
  (** [counter_of_objects decoder] returns the actual entry processed by the
      decoder. *)

  val hash : decoder -> digest
  (** [hash decoder] returns the actual (and computed by the decoder) signature
      of the PACK file. *)

  val src_rem : decoder -> int
  val src : decoder -> De.bigstring -> int -> int -> decoder

  val of_seq :
       output:De.bigstring
    -> allocate:(int -> De.window)
    -> ref_length:int
    -> digest:digest
    -> string Seq.t
    -> [ `Number of int | `Entry of entry | `Hash of string ] Seq.t
  (** [of_seq ~output ~allocate ~ref_length ~digest seq] analyses a PACK stream
      given by [seq] and returns a stream of all the entries in the given PACK
      stream as well as the final signature of the PACK stream. Several values
      are expected:
      - [output] is a temporary buffer used to decompress the inputs
      - [allocate] is a function used to allocate a window needed for
        decompression
      - [ref_length] is the size (in bytes) of the unique identifier of an
        object (for Git, this size is [20], the size of a SHA1 hash)
      - [digest] is the algorithm used to check the integrity of the stream PACK
        (for Git, the algorithm is SHA1, see {!type:hash}) *)
end

(** {1 Extracting objects from a PACK file.}

    Once it is possible to use [Unix.map_file] (or equivalent) on a PACK file
    (i.e. once it is available in a file system), it is possible to extract all
    the objects in this PACK file.

    Extraction consists of either:
    - decompressing a {!const:First_pass.Base} entry
    - decompressing a {i patch} and reconstructing the object from a source

    In both cases, we use [bigstring]s. The advantage of the latter is that they
    are not relocated by the OCaml GC. The disadvantage is their allocation (via
    [malloc()]), which can take a long time.

    Memory usage is also a disadvantage. If an object is 1 Go in size, we are
    obliged to allocate a [bigstring] of 1 Go (or more). It is not possible to
    {i stream-out} all objects - only {!const:First_pass.Base} objects can be
    streamed-out.

    To limit the use of [bigstring]s, there are various functions that let you
    know in advance:
    - the true size of the object requested
    - the size needed to store the requested object and all the objects needed
      to rebuild it if it is stored as a {i patch}
    - the list of objects that need to be rebuilt to construct the requested
      object

    As far as {i patch} entries are concerned ({!const:First_pass.Ofs} and
    {!const:First_pass.Ref}), their source can also be an object from a
    {i patch} which itself requires an object from a {i patch}. This is referred
    to as the {i depth} of the object in the PACK file. The maximum depth is 50:
    in other words, it may be necessary to reconstruct 49 objects upstream in
    order to build the requested object.

    The advantage is, of course, the compression ratio. In addition to
    compressing the entries with [zlib], some objects are just patches compared
    to other objects. For example, if the PACK file contains a {i blob} with
    content [A] and another {i blob} with content [A+B], the latter could be a
    patch containing only [+B] and requiring our first {i blob} as a source.

    For simple use, the user must first calculate the size of the buffers needed
    to store the object in memory. They then need to allocate a {!module:Blob}
    to hold the object. Finally, the object can be reconstructed according to
    its position ([cursor]) in the PACK file or according to its unique
    identifier if the user has the IDX file that allows the position of the
    object in the PACK file to be associated with its identifier (see
    {!module:Classeur}).

    {[
      let t = Carton.make ~map ~z ~allocate ~ref_length in
      let size = Carton.size_of_offset t ~cursor in
      let blob = Carton.Blob.make ~size in
      Carton.of_offset t blob ~cursor
    ]} *)

type 'fd t
(** Type representing a PACK file and used to extract objects. *)

val make :
     ?pagesize:int
  -> ?cachesize:int
  -> map:'fd Cachet.map
  -> 'fd
  -> z:Zl.bigstring
  -> allocate:(int -> Zl.window)
  -> ref_length:int
  -> (Uid.t -> int)
  -> 'fd t
(** [make ~map fd ~z ~allocate ~ref_length where] creates a representation of
    the PACK file whose read access is managed by the [map] function. A few
    arguments are required so that Carton does not allocate buffers arbitrarily
    but gives the user fine-grained control over its allocation policy (since it
    essentially involves allocating [bigstrings]).
    - A temporary buffer [z] is required to store a {i deflated} entry
    - an [allocate] function is required to get a [Zl.window] required to
      deflate entries
    - it is necessary to know the size [ref_length] of the unique identifiers
      that can be used to refer to patches. In the case of Git, this value is
      [20] (the size of a SHA1 hash)
    - lastly, a function [where] {b may} be required to find out the position of
      an object according to its unique identifier (see {!module:Classeur}).

    {b Note}: If [where] is proposed and exhaustive, the [*of_uid*] functions
    can be used.

    [make] calls [Cachet.make] with the [cachesize] and [pagesize] arguments.
    These must be multiples of 2. For more details about these arguments and
    [map], please refer to the Cachet documentation. *)

val of_cache :
     'fd Cachet.t
  -> z:Zl.bigstring
  -> allocate:(int -> Zl.window)
  -> ref_length:int
  -> (Uid.t -> int)
  -> 'fd t
(** [of_cache cache ~z ~allocate ~ref_length where] is equivalent to {!val:make}
    but uses the [cache] already available and initialised by the user. *)

val copy : 'fd t -> 'fd t
(** [copy t] makes a copy of the PACK file representation, which implies a new
    empty cache and a copy of the internal buffers. In this way, the result of
    this copy can be used {b in parallel} safely, even if our first value [t]
    attempts to extract objects at the same time. *)

val fd : 'fd t -> 'fd
(** [fd t] returns the file-descriptor given by the user to make the
    representation of the PACK file [t]. *)

val cache : 'fd t -> 'fd Cachet.t
(** [cache t] returns the cache used to access pages in the PACK file. *)

val allocate : 'fd t -> int -> Zl.window
val tmp : 'fd t -> De.bigstring
val ref_length : 'fd t -> int
val map : 'fd t -> cursor:int -> Cachet.Bstr.t
val with_index : 'fd t -> (Uid.t -> int) -> 'fd t

(* {2 Reconstruct a patch.}

   Carton can be used to reconstruct a requested object using a {!module:Blob}.
   A blob contains 2 temporary buffers, enabling a patch to be applied to one
   of these buffers while the other is the source. In short, one buffer
   contains the result of the patch while the other buffer contains the source
   needed to apply the patch.

   As mentioned, an object can require a succession of patches (depending on
   its depth). In short, it is possible to use a single {!type:Blob.t} to apply
   this succession of patches and {i flip} what was the result of one of the
   patches into the source required for the next patch. In this way, only twice
   the size of the object is needed in bytes to rebuild it, whatever its depth.

   However, you need to be sure that the succession of sources that you are
   about to rebuild fits into our {!type:Blob.t} buffers. It can happen that a
   source is larger than the object resulting from the application of the patch
   (and vice versa). So we need to calculate the necessary size of our Blob's
   buffers by collecting all the patches needed to rebuild the requested object
   down to the {i Base}.

   This size corresponds to the {!module:Size} module and does not correspond
   to the size of the object but to the size of the buffers needed to rebuild
   the object. *)

(** The [Blob] is a tuple of temporary buffers used to store an object that has
    been decompressed or reconstructed using a patch and a source.

    The purpose of a Blob is to prepare the allocation of what is needed to
    store the object and (re)use this [Blob] throughout the reconstruction of
    the object (whether it's a base or a patch) in order to obtain the final
    object. *)
module Blob : sig
  type t
  (** The type of blobs. *)

  val make : size:Size.t -> t
  val of_string : string -> t
  val size : t -> Size.t
  val source : t -> De.bigstring
  val with_source : t -> source:De.bigstring -> t
  val payload : t -> De.bigstring
  val flip : t -> t
end

module Visited : sig
  type t
end

exception Cycle
exception Too_deep
exception Bad_type

val size_of_offset :
  'fd t -> ?visited:Visited.t -> cursor:int -> Size.t -> Size.t
(** [size_of_uid pack ?visited ~cursor size] returns the size of the buffers
    (see {!module:Blob}s) required to extract the object located at [cursor]
    from the PACK file. This does {b not} correspond to the size of the object.

    The given [pack] must be able to recognize the object's position based on
    its unique identifier. *)

val size_of_uid : 'fd t -> ?visited:Visited.t -> uid:Uid.t -> Size.t -> Size.t
(** [size_of_uid pack ?visited ~uid size] returns the size of the buffers (see
    {!module:Blob}s) required to extract the object identified by [uid] from the
    PACK file. This does {b not} correspond to the size of the object.

    The given [pack] must be able to recognize the object's position based on
    its unique identifier. In other words, [pack] must be constructed with an
    exhaustive [where] function for all the identifiers in the PACK file. *)

val actual_size_of_offset : 'fd t -> cursor:int -> Size.t
(** [actual_size_of_offset pack ~cursor] returns the {b true} size of the object
    located at cursor in the given [pack] PACK file. *)

module Value : sig
  type t

  val kind : t -> Kind.t
  val bigstring : t -> De.bigstring
  val source : t -> De.bigstring
  val with_source : t -> source:De.bigstring -> t
  val string : t -> string
  val length : t -> int
  val depth : t -> int
  val blob : t -> Blob.t
  val flip : t -> t
  val make : kind:Kind.t -> ?depth:int -> De.bigstring -> t
  val of_string : kind:Kind.t -> ?depth:int -> string -> t
  val of_blob : kind:Kind.t -> length:int -> ?depth:int -> Blob.t -> t
  val pp : Format.formatter -> t -> unit
end

val of_offset : 'fd t -> Blob.t -> cursor:int -> Value.t
(** [of_offset pack blob ~cursor] is the object at the offset [cursor] into
    [pack]. The function is not {i tail-recursive}. It discovers at each step if
    the object depends on another one (see [OBJ_REF_DELTA] or [OBJ_OFS_DELTA]).

    {b Note}: This function does not allocate larges resources (or, at least,
    only the given [allocate] function to {!type:t} is able to allocate a large
    resource). [blob] (which should be created with the associated
    {!type:Size.t} given by {!size_of_offset}) is enough to extract the object.

    {b Note}: This function is {b not} tail-recursive. In other words, it can
    discover, step by step, the patches needed to rebuild the object. Even
    though a well-formed PACK file should not contain objects deeper than [50],
    if you want to rebuild an object and are sure that the function is
    tail-recursive, you need to calculate its {!type:Path.t} first. *)

val of_uid : 'fd t -> Blob.t -> uid:Uid.t -> Value.t
(** As {!of_offset}, [of_uid pack block ~uid] is the object identified by [uid]
    into [pack]. *)

(** {3:path Path of object.}

    Due to the fact that {!val:of_offset}/{!val:of_uid} are not {i tail-rec}, an
    other solution exists to extract an object from the PACK file. However, this
    solution requires a {i meta-data} {!type:Path.t} to be able to extract an
    object.

    A {!type:Path.t} is the {i delta-chain} of the object. It assumes that a
    {i delta-chain} can not be larger than [50] (see Git assumptions). From it,
    the way to construct an object is well-know and the step to discover if an
    object depends on an other one is deleted - and we ensure that the
    reconstruction is bound over our {!type:Path.t}. *)

module Path : sig
  type t
  (** Type of paths. *)

  val to_list : t -> int list
  val kind : t -> Kind.t
  val size : t -> Size.t
end

val path_of_offset : ?max_depth:int -> 'fd t -> cursor:int -> Path.t
(** [path_of_offset sched ~map t ~cursor] is that {!path} of the given object
    available at [cursor].

    {b Note.} This function can try to partially inflate objects. So, this
    function can use internal buffers and it is not {i thread-safe}.

    {b Note.} This function can try to {i look-up} an other object if it
    extracts an [OBJ_REF_DELTA] object. However, if we suppose that we process a
    PACKv2, an [OBJ_REF_DELTA] {i usually} points to an external object (see
    {i thin}-pack). *)

val path_of_uid : 'fd t -> Uid.t -> Path.t
(** [path_of_uid sched ~map t uid] is the {!path} of the given object identified
    by [uid] into [t].

    {b Note.} As {!size_of_offset}, this function can inflate objects and use
    internal buffers and it is not {i thread-safe}.

    {b Note.} Despite {!size_of_offset}, this function {b look-up} the object
    from the given reference. *)

val of_offset_with_path :
  'fd t -> path:Path.t -> Blob.t -> cursor:int -> Value.t
(** [of_offset_with_path sched ~map t ~path raw ~cursor] is the object available
    at [cursor] into [t]. This function is {i tail-recursive} and bound to the
    given [path]. *)

val of_offset_with_source : 'fd t -> Value.t -> cursor:int -> Value.t
(** [of_offset_with_source ~map t ~path source ~cursor] is the object available
    at [cursor] into [t]. This function is {i tail-recursive} and use the given
    [source] if the requested object is a patch. *)

(** {3 Unique identifier of objects.}

    Unique identifier of objects is a user-defined type which is not described
    by the format of the PACK file. By this fact, the way to {i digest} an
    object is at the user's discretion. For example, Git {i prepends} the value
    by an header such as:

    {[
      let digest v =
        let kind = match kind v with
          | `A -> "commit"
          | `B -> "tree"
          | `C -> "blob"
          | `D -> "tag" in
        let hdr = Fmt.str "%s %d\000" kind (len v) int
        let ctx = Digest.empty in
        Digest.feed_string ctx hdr ;
        Digest.feed_bigstring ctx (Bigarray.Array1.sub (raw v) 0 (len v)) ;
        Digest.finalize ctx
    ]}

    Of course, the user can decide how to digest a value (see {!identify}).
    However, 2 objects with the same contents but different types must have
    different unique identifiers. *)

type identify = kind:Kind.t -> ?off:int -> ?len:int -> De.bigstring -> Uid.t

val uid_of_offset :
  identify:identify -> 'fd t -> Blob.t -> cursor:int -> Kind.t * Uid.t

val uid_of_offset_with_source :
     identify:identify
  -> 'fd t
  -> kind:Kind.t
  -> Blob.t
  -> depth:int
  -> cursor:int
  -> Uid.t

type children = cursor:int -> uid:Uid.t -> int list
type where = cursor:int -> int

type oracle = {
    identify: identify
  ; children: children
  ; where: where
  ; size: cursor:int -> Size.t
  ; checksum: cursor:int -> Optint.t
  ; is_base: pos:int -> int option
  ; is_thin: bool
  ; number_of_objects: int
  ; hash: string
}

type status =
  | Unresolved_base of { cursor: int }
  | Unresolved_node
  | Resolved_base of { cursor: int; uid: Uid.t; crc: Optint.t; kind: Kind.t }
  | Resolved_node of {
        cursor: int
      ; uid: Uid.t
      ; crc: Optint.t
      ; kind: Kind.t
      ; depth: int
      ; parent: Uid.t
    }
