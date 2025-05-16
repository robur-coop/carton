let ( $ ) f g = fun x -> f (g x)

module Make (Hash : Digestif.S) = struct
  let pp_kind ppf = function
    | `A -> Format.pp_print_string ppf "commit"
    | `B -> Format.pp_print_string ppf "tree"
    | `C -> Format.pp_print_string ppf "blob"
    | `D -> Format.pp_print_string ppf "tag"

  let identify =
    let init kind (len : Carton.Size.t) =
      let hdr = Format.asprintf "%a %d\000" pp_kind kind (len :> int) in
      let ctx = Hash.empty in
      Hash.feed_string ctx hdr
    in
    let feed bstr ctx = Hash.feed_bigstring ctx bstr in
    let serialize = Hash.(Carton.Uid.unsafe_of_string $ to_raw_string $ get) in
    { Carton.First_pass.init; feed; serialize }

  let config ?threads ?on_entry ?on_object () =
    Carton_lwt.config ?threads ?on_entry ?on_object ~ref_length:Hash.digest_size
      (Carton.Identify identify)

  let hash =
    let feed_bytes buf ~off ~len ctx = Hash.feed_bytes ctx buf ~off ~len in
    let feed_bigstring bstr ctx = Hash.feed_bigstring ctx bstr in
    let serialize = Hash.(to_raw_string $ get) in
    let length = Hash.digest_size in
    { Carton.First_pass.feed_bytes; feed_bigstring; serialize; length }

  let digest = Carton.First_pass.Digest (hash, Hash.empty)

  let verify_from_stream ~cfg ~append cache stream =
    Carton_lwt.verify_from_stream ~cfg ~digest ~append cache stream

  let delta ~load entries =
    Carton_lwt.delta ~ref_length:Hash.digest_size ~load entries

  let to_pack ?with_header ?with_signature ?cursor ?level ~load targets =
    let with_signature =
      match with_signature with
      | Some ctx -> Some (Carton.First_pass.Digest (hash, ctx))
      | None -> None
    in
    Carton_lwt.to_pack ?with_header ?with_signature ?cursor ?level ~load targets

  let index ~length cache =
    Carton_lwt.index ~length ~hash_length:Hash.digest_size
      ~ref_length:Hash.digest_size cache

  let make ?z ?index cache =
    Carton_lwt.make ?z ~ref_length:Hash.digest_size ?index cache

  let entries_to_index ~pack entries =
    let fn = function
      | Carton.Unresolved_base _ | Carton.Unresolved_node _ ->
          failwith "Unresolved entry"
      | Resolved_base { cursor; uid; crc; _ } ->
          let uid = Classeur.unsafe_uid_of_string (uid :> string) in
          Classeur.Encoder.{ uid; crc; offset= Int64.of_int cursor }
      | Resolved_node { cursor; uid; crc; _ } ->
          let uid = Classeur.unsafe_uid_of_string (uid :> string) in
          Classeur.Encoder.{ uid; crc; offset= Int64.of_int cursor }
    in
    let entries = Array.map fn entries in
    let encoder =
      Classeur.Encoder.encoder `Manual ~digest ~pack
        ~ref_length:Hash.digest_size entries
    in
    let buf = Bytes.create 0x7ff in
    Classeur.Encoder.dst encoder buf 0 (Bytes.length buf);
    let rec go (`Await as await) =
      match Classeur.Encoder.encode encoder await with
      | `Ok ->
          let len = Bytes.length buf - Classeur.Encoder.dst_rem encoder in
          let str = Bytes.sub_string buf 0 len in
          Seq.Cons (str, Fun.const Seq.Nil)
      | `Partial ->
          let len = Bytes.length buf - Classeur.Encoder.dst_rem encoder in
          let str = Bytes.sub_string buf 0 len in
          let next () = go await in
          Seq.Cons (str, next)
    in
    Lwt_seq.of_seq (fun () -> go `Await)
end
