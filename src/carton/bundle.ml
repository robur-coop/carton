let invalid_argf fmt = Format.kasprintf invalid_arg fmt
let msgf fmt = Format.kasprintf (fun msg -> `Msg msg) fmt
let error_msgf fmt = Format.kasprintf (fun msg -> Error (`Msg msg)) fmt
let guard ~err fn = if fn () then Ok () else Error err
let ( let* ) = Result.bind

let for_all fn str =
  let res = ref true in
  for i = 0 to String.length str - 1 do
    res := !res && fn str.[i]
  done;
  !res

let rec iter_result fn = function
  | [] -> Ok ()
  | x :: r ->
      let* () = fn x in
      iter_result fn r

type version = [ `V2 | `V3 ]

type capability =
  [ `Object_format of string
  | `Filter of string
  | `Unknown of string * string option ]

type t = {
    version: version
  ; ref_length: int
  ; capabilities: capability list
  ; prerequisites: (Carton.Uid.t * string option) list
  ; references: (string * Carton.Uid.t) list
}

let signature_of_version = function
  | `V2 -> "# v2 git bundle"
  | `V3 -> "# v3 git bundle"

let rec object_format = function
  | [] -> None
  | `Object_format value :: _ -> Some value
  | _ :: capabilities -> object_format capabilities

let ref_length_of_capabilities ?forced capabilities =
  match forced with
  | Some ref_length -> ref_length
  | None ->
      begin match object_format capabilities with
      | Some "sha256" -> 32
      | Some _ | None -> 20
      end

let version { version; _ } = version
let capabilities { capabilities; _ } = capabilities
let prerequisites { prerequisites; _ } = prerequisites
let references { references; _ } = references
let ref_length { ref_length; _ } = ref_length

let is_thin { prerequisites; _ } =
  match prerequisites with [] -> false | _ :: _ -> true

let is_key_char = function
  | 'a' .. 'z' | 'A' .. 'Z' | '0' .. '9' | '-' -> true
  | _ -> false

let is_refname_char = function
  | ' ' | '\n' | '\r' | '\000' | '\127' -> false
  | chr -> Char.code chr >= 0x20

let key_of_capability = function
  | `Object_format _ -> "object-format"
  | `Filter _ -> "filter"
  | `Unknown (key, _) -> key

let value_of_capability = function
  | `Object_format value | `Filter value -> Some value
  | `Unknown (_, value) -> value

let check_capability capability =
  let key = key_of_capability capability in
  let* () =
    guard ~err:(msgf "Bundle: empty capability key") @@ fun () ->
    String.length key > 0
  in
  let* () =
    guard ~err:(msgf "Bundle: invalid capability key %S" key) @@ fun () ->
    for_all is_key_char key
  in
  match value_of_capability capability with
  | Some value ->
      guard ~err:(msgf "Bundle: the value of %S contains a newline" key)
      @@ fun () -> String.contains value '\n' = false
  | None -> Ok ()

let check_uid ~ref_length uid =
  guard
    ~err:
      (msgf "Bundle: %a is not a %d-byte(s) identifier" Carton.Uid.pp uid
         ref_length)
  @@ fun () -> String.length (uid : Carton.Uid.t :> string) = ref_length

let check_prerequisite ~ref_length (uid, comment) =
  let* () = check_uid ~ref_length uid in
  match comment with
  | None -> Ok ()
  | Some comment ->
      guard ~err:(msgf "Bundle: a prerequisite comment contains a newline")
      @@ fun () -> String.contains comment '\n' = false

let check_reference ~ref_length (name, uid) =
  let* () = check_uid ~ref_length uid in
  let* () =
    guard ~err:(msgf "Bundle: empty reference name") @@ fun () ->
    String.length name > 0
  in
  guard ~err:(msgf "Bundle: invalid reference name %S" name) @@ fun () ->
  for_all is_refname_char name

let check t =
  let ref_length = t.ref_length in
  let* () =
    guard ~err:(msgf "Bundle: invalid identifier length %d" ref_length)
    @@ fun () -> ref_length > 0
  in
  let* () =
    guard ~err:(msgf "Bundle: a v2 bundle can not advertise capabilities")
    @@ fun () ->
    match (t.version, t.capabilities) with `V2, _ :: _ -> false | _ -> true
  in
  let* () = iter_result check_capability t.capabilities in
  let* () = iter_result (check_prerequisite ~ref_length) t.prerequisites in
  iter_result (check_reference ~ref_length) t.references

let make ?version ?ref_length ?(capabilities = []) ?(prerequisites = [])
    references =
  let version =
    match (version, capabilities) with
    | Some version, _ -> version
    | None, [] -> `V2
    | None, _ :: _ -> `V3
  in
  let ref_length = ref_length_of_capabilities ?forced:ref_length capabilities in
  let t = { version; ref_length; capabilities; prerequisites; references } in
  match check t with Ok () -> t | Error (`Msg msg) -> invalid_argf "%s" msg

let pp_capability ppf capability =
  match value_of_capability capability with
  | Some value ->
      Format.fprintf ppf "@@%s=%s" (key_of_capability capability) value
  | None -> Format.fprintf ppf "@@%s" (key_of_capability capability)

let pp ppf t =
  Format.fprintf ppf "%s@\n" (signature_of_version t.version);
  List.iter (fun c -> Format.fprintf ppf "%a@\n" pp_capability c) t.capabilities;
  List.iter
    (fun (uid, comment) ->
      match comment with
      | Some comment -> Format.fprintf ppf "-%a %s@\n" Carton.Uid.pp uid comment
      | None -> Format.fprintf ppf "-%a@\n" Carton.Uid.pp uid)
    t.prerequisites;
  List.iter
    (fun (name, uid) -> Format.fprintf ppf "%a %s@\n" Carton.Uid.pp uid name)
    t.references

(* Encoder *)

let to_string t =
  begin match check t with
  | Ok () -> ()
  | Error (`Msg msg) -> invalid_argf "%s" msg
  end;
  let buf = Buffer.create 0x100 in
  Buffer.add_string buf (signature_of_version t.version);
  Buffer.add_char buf '\n';
  List.iter
    (fun capability ->
      Buffer.add_char buf '@';
      Buffer.add_string buf (key_of_capability capability);
      begin match value_of_capability capability with
      | Some value ->
          Buffer.add_char buf '=';
          Buffer.add_string buf value
      | None -> ()
      end;
      Buffer.add_char buf '\n')
    t.capabilities;
  List.iter
    (fun (uid, comment) ->
      Buffer.add_char buf '-';
      Buffer.add_string buf (Ohex.encode (uid : Carton.Uid.t :> string));
      begin match comment with
      | Some comment ->
          Buffer.add_char buf ' ';
          Buffer.add_string buf comment
      | None -> ()
      end;
      Buffer.add_char buf '\n')
    t.prerequisites;
  List.iter
    (fun (name, uid) ->
      Buffer.add_string buf (Ohex.encode (uid : Carton.Uid.t :> string));
      Buffer.add_char buf ' ';
      Buffer.add_string buf name;
      Buffer.add_char buf '\n')
    t.references;
  Buffer.add_char buf '\n';
  Buffer.contents buf

let to_seq t pack = Seq.cons (to_string t) pack

(* Decoder *)

type src = [ `String of string | `Manual ]

type decoder = {
    src: src
  ; input: Bstr.t
  ; input_pos: int
  ; input_len: int
  ; buf: Buffer.t
  ; version: version
  ; forced_ref_length: int option
  ; capabilities: capability list
  ; prerequisites: (Carton.Uid.t * string option) list
  ; references: (string * Carton.Uid.t) list
  ; max_header_size: int
  ; header_size: int
  ; k: decoder -> decode
}

and decode =
  [ `Await of decoder
  | `Header of t * decoder
  | `Pack of string * decoder
  | `End
  | `Malformed of string ]

let src_rem decoder = decoder.input_len - decoder.input_pos + 1

let end_of_input decoder =
  { decoder with input= Bstr.empty; input_pos= 0; input_len= min_int }

let malformedf fmt = Format.kasprintf (fun err -> `Malformed err) fmt
let malformed (`Msg msg) = `Malformed msg

let refill k decoder =
  match decoder.src with
  | `String _ -> k (end_of_input decoder)
  | `Manual -> `Await { decoder with k }

let rec line k decoder =
  let rem = src_rem decoder in
  if rem < 0 then k None decoder
  else if rem == 0 then refill (line k) decoder
  else
    let consumed len =
      let decoder = { decoder with header_size= decoder.header_size + len } in
      let* () =
        guard
          ~err:
            (msgf "Bundle: the header is larger than %d byte(s)"
               decoder.max_header_size)
        @@ fun () -> decoder.header_size <= decoder.max_header_size
      in
      Ok decoder
    in
    match Bstr.index decoder.input ~off:decoder.input_pos ~len:rem '\n' with
    | Some idx -> begin
        let len = idx - decoder.input_pos in
        Buffer.add_string decoder.buf
          (Bstr.sub_string decoder.input ~off:decoder.input_pos ~len);
        match consumed (len + 1) with
        | Error err -> malformed err
        | Ok decoder ->
            let str = Buffer.contents decoder.buf in
            Buffer.clear decoder.buf;
            k (Some str) { decoder with input_pos= idx + 1 }
      end
    | None -> begin
        Buffer.add_string decoder.buf
          (Bstr.sub_string decoder.input ~off:decoder.input_pos ~len:rem);
        match consumed rem with
        | Error err -> malformed err
        | Ok decoder ->
            let decoder = { decoder with input_pos= decoder.input_pos + rem } in
            refill (line k) decoder
      end

let capability_of_string str =
  let key, value =
    match String.index_opt str '=' with
    | Some idx ->
        let len = String.length str - idx - 1 in
        (String.sub str 0 idx, Some (String.sub str (idx + 1) len))
    | None -> (str, None)
  in
  let capability =
    match (key, value) with
    | "object-format", Some value -> `Object_format value
    | "filter", Some value -> `Filter value
    | _ -> `Unknown (key, value)
  in
  let* () = check_capability capability in
  Ok capability

let uid_of_hex str =
  match Ohex.decode ~skip_whitespace:false str with
  | str -> Ok (Carton.Uid.unsafe_of_string str)
  | exception Invalid_argument _ ->
      error_msgf "Bundle: invalid object identifier %S" str

let expected_ref_length decoder =
  ref_length_of_capabilities ?forced:decoder.forced_ref_length
    (List.rev decoder.capabilities)

let header_of_decoder decoder =
  {
    version= decoder.version
  ; ref_length= expected_ref_length decoder
  ; capabilities= List.rev decoder.capabilities
  ; prerequisites= List.rev decoder.prerequisites
  ; references= List.rev decoder.references
  }

let decode_entry str decoder =
  let is_prerequisite = str.[0] = '-' in
  let str =
    if is_prerequisite then String.sub str 1 (String.length str - 1) else str
  in
  let ref_length = expected_ref_length decoder in
  let* () =
    guard ~err:(msgf "Bundle: truncated entry %S" str) @@ fun () ->
    String.length str >= ref_length * 2
  in
  let hex = String.sub str 0 (ref_length * 2) in
  let rem =
    String.sub str (ref_length * 2) (String.length str - (ref_length * 2))
  in
  let* uid = uid_of_hex hex in
  match is_prerequisite with
  | true ->
      let* () =
        guard ~err:(msgf "Bundle: malformed prerequisite %S" str) @@ fun () ->
        String.length rem == 0 || rem.[0] = ' '
      in
      let comment =
        if String.length rem == 0 then None
        else Some (String.sub rem 1 (String.length rem - 1))
      in
      let prerequisites = (uid, comment) :: decoder.prerequisites in
      Ok { decoder with prerequisites }
  | false ->
      let* () =
        guard ~err:(msgf "Bundle: missing reference name for %s" hex)
        @@ fun () -> String.length rem > 0 && rem.[0] = ' '
      in
      let name = String.sub rem 1 (String.length rem - 1) in
      let* () = check_reference ~ref_length (name, uid) in
      let references = (name, uid) :: decoder.references in
      Ok { decoder with references }

let rec decode_pack decoder =
  let rem = src_rem decoder in
  if rem < 0 then `End
  else if rem == 0 then refill decode_pack decoder
  else
    let str = Bstr.sub_string decoder.input ~off:decoder.input_pos ~len:rem in
    let decoder =
      { decoder with input_pos= decoder.input_pos + rem; k= decode_pack }
    in
    `Pack (str, decoder)

let rec decode_signature str decoder =
  match str with
  | None -> malformedf "Bundle: unexpected end of input (signature)"
  | Some "# v2 git bundle" -> line decode_refs { decoder with version= `V2 }
  | Some "# v3 git bundle" ->
      line decode_capabilities { decoder with version= `V3 }
  | Some str -> malformedf "Bundle: invalid signature %S" str

and decode_capabilities str decoder =
  match str with
  | None -> malformedf "Bundle: unexpected end of input (capabilities)"
  | Some str' when String.length str' > 0 && str'.[0] = '@' -> begin
      let str' = String.sub str' 1 (String.length str' - 1) in
      match capability_of_string str' with
      | Error err -> malformed err
      | Ok capability ->
          let capabilities = capability :: decoder.capabilities in
          line decode_capabilities { decoder with capabilities }
    end
  | Some _ -> decode_refs str decoder

and decode_refs str decoder =
  match str with
  | None -> malformedf "Bundle: unexpected end of input (references)"
  | Some "" ->
      let t = header_of_decoder decoder in
      `Header (t, { decoder with k= decode_pack })
  | Some str ->
      begin match decode_entry str decoder with
      | Ok decoder -> line decode_refs decoder
      | Error err -> malformed err
      end

let decoder ?ref_length ?(max_header_size = 0x100000) src =
  let input, input_pos, input_len =
    match src with
    | `Manual -> (Bstr.empty, 1, 0)
    | `String str -> (Bstr.of_string str, 0, String.length str - 1)
  in
  {
    src
  ; input
  ; input_pos
  ; input_len
  ; buf= Buffer.create 0x100
  ; version= `V2
  ; forced_ref_length= ref_length
  ; capabilities= []
  ; prerequisites= []
  ; references= []
  ; max_header_size
  ; header_size= 0
  ; k= line decode_signature
  }

let decode decoder = decoder.k decoder

let src decoder bstr idx len =
  if idx < 0 || len < 0 || idx + len > Bstr.length bstr then
    invalid_argf "Bundle.src: source out of bounds";
  if len == 0 then end_of_input decoder
  else { decoder with input= bstr; input_pos= idx; input_len= idx + len - 1 }

let of_seq ?ref_length ?max_header_size seq =
  let input = Bstr.create 0x1000 in
  let rec go decoder seq (str, src_off, src_len) () =
    match decode decoder with
    | `Await decoder ->
        if src_len == 0 then
          begin match Seq.uncons seq with
          | Some (str, seq) ->
              let len = Int.min (Bstr.length input) (String.length str) in
              Bstr.blit_from_string str ~src_off:0 input ~dst_off:0 ~len;
              let decoder = src decoder input 0 len in
              go decoder seq (str, len, String.length str - len) ()
          | None ->
              let decoder = src decoder Bstr.empty 0 0 in
              go decoder seq (String.empty, 0, 0) ()
          end
        else begin
          let len = Int.min (Bstr.length input) src_len in
          Bstr.blit_from_string str ~src_off input ~dst_off:0 ~len;
          let decoder = src decoder input 0 len in
          go decoder seq (str, src_off + len, src_len - len) ()
        end
    | `Header (t, decoder) ->
        let next = go decoder seq (str, src_off, src_len) in
        Seq.Cons (`Header t, next)
    | `Pack (payload, decoder) ->
        let next = go decoder seq (str, src_off, src_len) in
        Seq.Cons (`Pack payload, next)
    | `End -> Seq.Nil
    | `Malformed err -> failwith err
  in
  let decoder = decoder ?ref_length ?max_header_size `Manual in
  go decoder seq (String.empty, 0, 0)

let split ?ref_length ?max_header_size seq =
  let seq = of_seq ?ref_length ?max_header_size seq in
  match Seq.uncons seq with
  | Some (`Header t, seq) ->
      let fn = function `Pack str -> str | `Header _ -> assert false in
      Ok (t, Seq.map fn seq)
  | Some (`Pack _, _) -> error_msgf "Bundle: unexpected PACK stream"
  | None -> error_msgf "Bundle: empty stream"
  | exception Failure err -> Error (`Msg err)
