(** Read and produce {i git-bundle} files. *)

val of_packs :
     cfg:Carton_miou_unix.config
  -> digest:Carton.First_pass.digest
  -> sort:Carton_miou_unix.sort
  -> ?level:int
  -> Bundle.t
  -> Fpath.t list
  -> string Seq.t
(** [of_packs ~cfg ~digest ~sort hdr packs] is the bundle stream made of the
    header [hdr] followed by a PACK re-delta-ified from the given [packs] (see
    {!val:Carton_miou_unix.merge}). *)

val read :
     ?ref_length:int
  -> Fpath.t
  -> (Bundle.t * string Seq.t, [> `Msg of string ]) result
(** [read filename] is the header of the bundle [filename] and the stream of the
    PACK bytes it contains.

    {b Note}: the returned sequence is {b not} persistent. *)

val split :
     ?ref_length:int
  -> Fpath.t
  -> pack:Fpath.t
  -> (Bundle.t, [> `Msg of string ]) result
(** [split filename ~pack] extracts the PACK contained into the bundle
    [filename] to [pack] and returns the header of the bundle.

    {b Note}: if the bundle has prerequisites ({!val:Bundle.is_thin}), the
    extracted PACK is {i thin} and the missing sources must be given back to
    Carton as {!const:Carton.Extern} objects. *)

(**/*)

val save : Fpath.t -> string Seq.t -> unit
