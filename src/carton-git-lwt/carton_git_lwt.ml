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
end
