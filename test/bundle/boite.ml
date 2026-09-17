let ( $ ) f g x = f (g x)

let git_identify =
  let open Digestif in
  let pp_kind ppf = function
    | `A -> Format.pp_print_string ppf "commit"
    | `B -> Format.pp_print_string ppf "tree"
    | `C -> Format.pp_print_string ppf "blob"
    | `D -> Format.pp_print_string ppf "tag"
  in
  let init kind (len : Carton.Size.t) =
    let hdr = Format.asprintf "%a %d\000" pp_kind kind (len :> int) in
    let ctx = SHA1.empty in
    SHA1.feed_string ctx hdr
  in
  let feed bstr ctx = SHA1.feed_bigstring ctx bstr in
  let serialize = SHA1.(Carton.Uid.unsafe_of_string $ to_raw_string $ get) in
  { Carton.First_pass.init; feed; serialize }

let sha1 =
  let open Digestif.SHA1 in
  let feed_bytes buf ~off ~len ctx = feed_bytes ctx ~off ~len buf in
  let feed_bigstring bstr ctx = feed_bigstring ctx bstr in
  let serialize = to_raw_string $ get in
  let hash =
    {
      Carton.First_pass.feed_bytes
    ; feed_bigstring
    ; serialize
    ; length= digest_size
    }
  in
  Carton.First_pass.Digest (hash, empty)

let hex uid = Ohex.encode (uid : Carton.Uid.t :> string)

let uid_of_hex str =
  match Ohex.decode ~skip_whitespace:false str with
  | str -> Carton.Uid.unsafe_of_string str
  | exception Invalid_argument _ ->
      Format.eprintf "Invalid object identifier: %S\n%!" str;
      exit 1

let fpath str =
  match Fpath.of_string str with
  | Ok value -> value
  | Error (`Msg msg) ->
      Format.eprintf "%s\n%!" msg;
      exit 1

let handle = function
  | Ok value -> value
  | Error (`Msg msg) ->
      Format.eprintf "%s\n%!" msg;
      exit 1

let list_heads filename =
  let hdr, _seq = handle (Carton_miou_bundle.read (fpath filename)) in
  let fn (name, uid) = Format.printf "%s %s\n" (hex uid) name in
  List.iter fn (Bundle.references hdr)

let list_prerequisites filename =
  let hdr, _seq = handle (Carton_miou_bundle.read (fpath filename)) in
  let fn (uid, comment) =
    match comment with
    | Some comment -> Format.printf "-%s %s\n" (hex uid) comment
    | None -> Format.printf "-%s\n" (hex uid)
  in
  List.iter fn (Bundle.prerequisites hdr)

let info filename =
  let hdr, _seq = handle (Carton_miou_bundle.read (fpath filename)) in
  let version = match Bundle.version hdr with `V2 -> 2 | `V3 -> 3 in
  Format.printf "version: %d\n" version;
  Format.printf "ref-length: %d\n" (Bundle.ref_length hdr);
  Format.printf "thin: %b\n" (Bundle.is_thin hdr);
  Format.printf "references: %d\n" (List.length (Bundle.references hdr));
  Format.printf "prerequisites: %d\n" (List.length (Bundle.prerequisites hdr))

let split filename pack =
  handle (Carton_miou_bundle.split (fpath filename) ~pack:(fpath pack))
  |> ignore

type args = {
    output: string option
  ; pack: string option
  ; references: (string * Carton.Uid.t) list
  ; prerequisites: (Carton.Uid.t * string option) list
  ; capabilities: Bundle.capability list
  ; version: Bundle.version option
}

let empty =
  {
    output= None
  ; pack= None
  ; references= []
  ; prerequisites= []
  ; capabilities= []
  ; version= None
  }

let cut ~sep str =
  match String.index_opt str sep with
  | Some idx ->
      let len = String.length str - idx - 1 in
      Some (String.sub str 0 idx, String.sub str (idx + 1) len)
  | None -> None

let rec parse args = function
  | [] -> args
  | "-o" :: value :: rest -> parse { args with output= Some value } rest
  | "-p" :: value :: rest -> parse { args with pack= Some value } rest
  | "-r" :: value :: rest ->
      let name, uid =
        match cut ~sep:'=' value with
        | Some (name, uid) -> (name, uid_of_hex uid)
        | None ->
            Format.eprintf "Invalid reference: %S\n%!" value;
            exit 1
      in
      parse { args with references= (name, uid) :: args.references } rest
  | "-P" :: value :: rest ->
      let uid, comment =
        match cut ~sep:'=' value with
        | Some (uid, comment) -> (uid_of_hex uid, Some comment)
        | None -> (uid_of_hex value, None)
      in
      parse
        { args with prerequisites= (uid, comment) :: args.prerequisites }
        rest
  | "--object-format" :: value :: rest ->
      let capabilities = `Object_format value :: args.capabilities in
      parse { args with capabilities } rest
  | "--v3" :: rest -> parse { args with version= Some `V3 } rest
  | arg :: _ ->
      Format.eprintf "Invalid argument: %S\n%!" arg;
      exit 1

let header_of_args args =
  Bundle.make ?version:args.version
    ~capabilities:(List.rev args.capabilities)
    ~prerequisites:(List.rev args.prerequisites)
    (List.rev args.references)

let required name = function
  | Some value -> value
  | None ->
      Format.eprintf "Missing %s\n%!" name;
      exit 1

let create argv =
  let args = parse empty argv in
  let hdr = header_of_args args in
  let pack = fpath (required "-p" args.pack) in
  let output = fpath (required "-o" args.output) in
  let seq = Bundle.to_seq hdr (Carton_miou_unix.seq_of_filename pack) in
  Carton_miou_bundle.save output seq

let sort entries =
  let entries = Array.concat entries in
  let set = Hashtbl.create 0x7ff in
  let cnt = ref 0 in
  let fn entry =
    let keep = Hashtbl.mem set (Cartonnage.Entry.uid entry) = false in
    if keep then begin
      Hashtbl.add set (Cartonnage.Entry.uid entry) ();
      incr cnt
    end;
    keep
  in
  let lst = List.filter fn (Array.to_list entries) in
  (!cnt, List.to_seq lst)

let sort = { Carton_miou_unix.sort }

let repack argv =
  Miou_unix.run @@ fun () ->
  let args = parse empty argv in
  let hdr = header_of_args args in
  let pack = fpath (required "-p" args.pack) in
  let output = fpath (required "-o" args.output) in
  let ref_length = Digestif.SHA1.digest_size in
  let identify = Carton.Identify git_identify in
  let cfg = Carton_miou_unix.config ~threads:1 ~ref_length identify in
  let seq = Carton_miou_bundle.of_packs ~cfg ~digest:sha1 ~sort hdr [ pack ] in
  Carton_miou_bundle.save output seq

let gen_uid ~ref_length n =
  let fn idx = Char.chr ((n + (idx * 7)) land 0xff) in
  let str = String.init ref_length fn in
  Carton.Uid.unsafe_of_string str

let roundtrip () =
  let check hdr =
    let str = Bundle.to_string hdr in
    let seq = Seq.return str in
    let hdr', seq = handle (Bundle.split seq) in
    assert (Bundle.version hdr' = Bundle.version hdr);
    assert (Bundle.ref_length hdr' = Bundle.ref_length hdr);
    assert (Bundle.capabilities hdr' = Bundle.capabilities hdr);
    assert (Bundle.prerequisites hdr' = Bundle.prerequisites hdr);
    assert (Bundle.references hdr' = Bundle.references hdr);
    assert (List.of_seq seq = []);
    assert (Bundle.to_string hdr' = str)
  in
  let gen_uid20 = gen_uid ~ref_length:20 in
  let gen_uid32 = gen_uid ~ref_length:32 in
  check (Bundle.make [ ("refs/heads/main", gen_uid20 0) ]);
  check (Bundle.make []);
  check
    (Bundle.make
       ~prerequisites:[ (gen_uid20 1, Some "a commit subject"); (gen_uid20 2, None) ]
       [ ("refs/heads/main", gen_uid20 0); ("refs/tags/v1.0.0", gen_uid20 3) ]);
  check
    (Bundle.make
       ~capabilities:[ `Object_format "sha256" ]
       ~prerequisites:[ (gen_uid32 1, Some "un sujet accentué") ]
       [ ("refs/heads/branche-accentuée", gen_uid32 0) ]);
  check
    (Bundle.make ~version:`V3
       ~capabilities:[ `Filter "blob:none"; `Unknown ("x-carton", None) ]
       [ ("HEAD", gen_uid20 4) ]);
  (* A PACK stream must survive the header, chunk boundaries included. *)
  let hdr = Bundle.make [ ("refs/heads/main", gen_uid20 0) ] in
  let pack = String.init 0x2000 (fun i -> Char.chr (i land 0xff)) in
  let chunks = [ Bundle.to_string hdr; pack ] in
  let hdr', seq = handle (Bundle.split (List.to_seq chunks)) in
  assert (Bundle.references hdr' = Bundle.references hdr);
  assert (String.concat "" (List.of_seq seq) = pack);
  (* The same, but with the header split across chunks. *)
  let str = Bundle.to_string hdr ^ pack in
  let chunks = List.init (String.length str) (fun i -> String.make 1 str.[i]) in
  let _hdr, seq = handle (Bundle.split (List.to_seq chunks)) in
  assert (String.concat "" (List.of_seq seq) = pack);
  print_endline "roundtrip: ok"

let usage () =
  prerr_endline "usage: bundle_test list-heads <bundle>";
  prerr_endline "       bundle_test list-prerequisites <bundle>";
  prerr_endline "       bundle_test info <bundle>";
  prerr_endline "       bundle_test split <bundle> <pack>";
  prerr_endline
    "       bundle_test create -o <bundle> -p <pack> [-r <name>=<oid>]";
  prerr_endline
    "       bundle_test repack -o <bundle> -p <pack> [-r <name>=<oid>]";
  prerr_endline "       bundle_test roundtrip";
  exit 1

let () =
  match List.tl (Array.to_list Sys.argv) with
  | [ "list-heads"; filename ] -> list_heads filename
  | [ "list-prerequisites"; filename ] -> list_prerequisites filename
  | [ "info"; filename ] -> info filename
  | [ "split"; filename; pack ] -> split filename pack
  | "create" :: argv -> create argv
  | "repack" :: argv -> repack argv
  | [ "roundtrip" ] -> roundtrip ()
  | _ -> usage ()
