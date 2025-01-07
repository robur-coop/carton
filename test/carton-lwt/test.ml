let ( $ ) f g = fun x -> f (g x)

let stream_of_filename filename =
  let ic = open_in filename in
  let bf = Bytes.create 0x7ff in
  let dispenser () =
    match input ic bf 0 (Bytes.length bf) with
    | 0 -> close_in ic; Lwt.return_none
    | len ->
        let str = Bytes.sub_string bf 0 len in
        Lwt.return_some str
    | exception End_of_file -> close_in ic; Lwt.return_none
  in
  Lwt_stream.from dispenser

let cache_of_filename filename =
  let fd = Unix.openfile filename Unix.[ O_RDONLY ] 0o644 in
  let stat = Unix.fstat fd in
  let map () ~pos len =
    let len = Int.min (stat.Unix.st_size - pos) len in
    let pos = Int64.of_int pos in
    let barr =
      Unix.map_file fd ~pos Bigarray.char Bigarray.c_layout false [| len |]
    in
    Bigarray.array1_of_genarray barr
  in
  at_exit (fun () -> Unix.close fd);
  Cachet.make ~map ()

let ref_length = Digestif.SHA1.digest_size

let identify =
  let open Digestif in
  let pp_kind ppf = function
    | `A -> Fmt.string ppf "commit"
    | `B -> Fmt.string ppf "tree"
    | `C -> Fmt.string ppf "blob"
    | `D -> Fmt.string ppf "tag"
  in
  let init kind (len : Carton.Size.t) =
    let hdr = Fmt.str "%a %d\000" pp_kind kind (len :> int) in
    let ctx = SHA1.empty in
    SHA1.feed_string ctx hdr
  in
  let feed bstr ctx = SHA1.feed_bigstring ctx bstr in
  let serialize = SHA1.(Carton.Uid.unsafe_of_string $ to_raw_string $ get) in
  { Carton.First_pass.init; feed; serialize }

let identify_value value =
  let kind = Carton.Value.kind value in
  let bstr = Carton.Value.bigstring value in
  let len = Carton.Value.length value in
  let ctx = identify.Carton.First_pass.init kind (Carton.Size.of_int_exn len) in
  let ctx =
    identify.Carton.First_pass.feed (Bigarray.Array1.sub bstr 0 len) ctx
  in
  identify.Carton.First_pass.serialize ctx

let digest =
  let feed_bigstring bstr ctx = Digestif.SHA1.feed_bigstring ctx bstr in
  let feed_bytes buf ~off ~len ctx =
    Digestif.SHA1.feed_bytes ctx ~off ~len buf
  in
  let hash =
    {
      Carton.First_pass.feed_bytes
    ; feed_bigstring
    ; serialize= Digestif.SHA1.to_raw_string $ Digestif.SHA1.get
    ; length= Digestif.SHA1.digest_size
    }
  in
  Carton.First_pass.Digest (hash, Digestif.SHA1.empty)

open Lwt

let status =
  let pp_status ppf = function
    | Carton.Unresolved_base { cursor } -> Fmt.pf ppf "%08x" cursor
    | Unresolved_node -> Fmt.string ppf "unresolved"
    | Resolved_base { uid; _ } -> Carton.Uid.pp ppf uid
    | Resolved_node { uid; _ } -> Carton.Uid.pp ppf uid
  in
  let equal_status a b =
    match (a, b) with
    | Carton.Unresolved_node, Carton.Unresolved_node -> true
    | Unresolved_base { cursor= a }, Unresolved_base { cursor= b } -> a = b
    | Resolved_base { uid= a; _ }, Resolved_base { uid= b; _ } ->
        Carton.Uid.equal a b
    | Resolved_node { uid= a; _ }, Resolved_node { uid= b; _ } ->
        Carton.Uid.equal a b
    | _ -> false
  in
  Alcotest.testable pp_status equal_status

let test00 =
  Alcotest_lwt.test_case "verify" `Quick @@ fun _sw _ ->
  let stream = stream_of_filename "bomb.pack" in
  let append _str ~off:_ ~len:_ = Lwt.return_unit in
  let cache = cache_of_filename "bomb.pack" in
  let cfg = Carton_lwt.config ~ref_length (Carton.Identify identify) in
  Carton_lwt.verify_from_stream ~cfg ~digest ~append cache stream
  >>= fun (matrix, _hash) ->
  let uid =
    Carton.Uid.unsafe_of_string
      (Ohex.decode "7af99c9e7d4768fa681f4fe4ff61259794cf719b")
  in
  Alcotest.(check status)
    "0xc" matrix.(0)
    (Carton.Resolved_base { cursor= 0; uid; crc= Optint.zero; kind= `A });
  let uid =
    Carton.Uid.unsafe_of_string
      (Ohex.decode "d9513477b01825130c48c4bebed114c4b2d50401")
  in
  let zero = Carton.Uid.unsafe_of_string (String.make ref_length '\000') in
  Alcotest.(check status)
    "0x5e4" matrix.(8)
    (Carton.Resolved_node
       { cursor= 0; uid; crc= Optint.zero; kind= `B; depth= 2; parent= zero });
  Lwt.return_unit

let entries_of_pack filename =
  let stream = stream_of_filename filename in
  let cache = cache_of_filename filename in
  let cfg = Carton_lwt.config ~ref_length (Carton.Identify identify) in
  let buf = Buffer.create 0x7ff in
  let append str ~off ~len =
    Buffer.add_substring buf str off len;
    Lwt.return_unit
  in
  Carton_lwt.verify_from_stream ~cfg ~digest ~append cache stream
  >>= fun (matrix, hash) ->
  let fn _idx = function
    | Carton.Unresolved_base _ | Carton.Unresolved_node -> assert false
    | Resolved_base { cursor; uid; crc; _ } ->
        let uid = Classeur.unsafe_uid_of_string (uid :> string) in
        Classeur.Encoder.{ uid; crc; offset= Int64.of_int cursor }
    | Resolved_node { cursor; uid; crc; _ } ->
        let uid = Classeur.unsafe_uid_of_string (uid :> string) in
        Classeur.Encoder.{ uid; crc; offset= Int64.of_int cursor }
  in
  Lwt.return (Array.mapi fn matrix, hash, Buffer.contents buf)

let hex =
  let pp ppf str = Fmt.pf ppf "%s" (Ohex.encode str) in
  let equal = String.equal in
  Alcotest.testable pp equal

let test01 =
  Alcotest_lwt.test_case "index" `Quick @@ fun _sw _ ->
  entries_of_pack "bomb.pack" >>= fun (entries, pack, payload) ->
  let out = Bytes.create 0x7ff in
  let encoder =
    Classeur.Encoder.encoder `Manual ~digest ~pack ~ref_length entries
  in
  let buf = Buffer.create 0x7ff in
  let rec go (`Await as await) =
    match Classeur.Encoder.encode encoder await with
    | `Ok ->
        let len = Bytes.length out - Classeur.Encoder.dst_rem encoder in
        Buffer.add_subbytes buf out 0 len
    | `Partial ->
        let len = Bytes.length out - Classeur.Encoder.dst_rem encoder in
        Buffer.add_subbytes buf out 0 len;
        Classeur.Encoder.dst encoder out 0 (Bytes.length out);
        go await
  in
  Classeur.Encoder.dst encoder out 0 (Bytes.length out);
  go `Await;
  let str = Buffer.contents buf in
  let map str ~pos len =
    let len = Int.min len (String.length str - pos) in
    Format.eprintf "map ~pos:%d %d\n%!" pos len;
    let str = String.sub str pos len in
    Carton.bigstring_of_string str
  in
  let idx = Cachet.make ~map str in
  let idx =
    Classeur.of_cachet ~length:(String.length str) ~hash_length:ref_length
      ~ref_length idx
  in
  Alcotest.(check hex)
    "signature" (Classeur.idx idx)
    (Ohex.decode "685b82116e4eb992f4f717eba7d5898410e0de76");
  let carton =
    let map str ~pos len =
      let len = Int.min len (String.length str - pos) in
      let str = String.sub str pos len in
      Carton.bigstring_of_string str
    in
    Cachet.make ~map payload
  in
  let carton = Carton_lwt.make ~ref_length carton in
  let uid = Ohex.decode "d9513477b01825130c48c4bebed114c4b2d50401" in
  let uid = Classeur.uid_of_string_exn idx uid in
  let cursor = Classeur.find_offset idx uid in
  let size = Carton.size_of_offset carton ~cursor Carton.Size.zero in
  let blob = Carton.Blob.make ~size in
  let path = Carton.path_of_offset carton ~cursor in
  let uid' =
    identify_value (Carton.of_offset_with_path carton ~path blob ~cursor)
  in
  Alcotest.(check hex) "find" (uid :> string) (uid' :> string);
  Lwt.return_unit

let () =
  Alcotest_lwt.run "carton-lwt" [ ("bomb", [ test00; test01 ]) ] |> Lwt_main.run
