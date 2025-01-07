let src = Logs.Src.create "cartonnage"

module Log = (val Logs.src_log src : Logs.LOG)

module Delta = struct
  type t = From of { level: int; src: Carton.Uid.t } | Zero

  let pp ppf = function
    | Zero -> Format.pp_print_string ppf "<none>"
    | From { src; _ } -> Format.fprintf ppf "@[<1>(From %a)@]" Carton.Uid.pp src
end

module Entry = struct
  type 'meta t = {
      uid: Carton.Uid.t
    ; kind: Carton.Kind.t
    ; length: int
    ; preferred: bool
    ; delta: Delta.t
    ; meta: 'meta
  }

  let make ~kind ~length ?(preferred = false) ?(delta = Delta.Zero) uid meta =
    { uid; kind; length; preferred; delta; meta }

  let length { length; _ } = length
  let uid { uid; _ } = uid
  let kind { kind; _ } = kind
  let meta { meta; _ } = meta

  let pp ppf t =
    Format.fprintf ppf
      "{ @[<hov>uid= %a;@ kind= %a;@ length= %d;@ preferred= %b;@ delta= \
       @[<hov>%a@];@] }"
      Carton.Uid.pp t.uid Carton.Kind.pp t.kind t.length t.preferred Delta.pp
      t.delta
end

module Source = struct
  type 'meta t = {
      index: Duff.index
    ; entry: 'meta Entry.t
    ; depth: int
    ; value: Carton.Value.t
  }

  let depth { depth; _ } = depth
  let uid { entry; _ } = Entry.uid entry
  let length { entry; _ } = Entry.length entry
  let kind { entry; _ } = Entry.kind entry
  let index { index; _ } = index

  let bigstring { value; _ } =
    let bstr = Carton.Value.bigstring value in
    Bigarray.Array1.sub bstr 0 (Carton.Value.length value)
end

module Patch = struct
  type t =
    | Patch of {
          hunks: Duff.hunk list
        ; depth: int
        ; source: Carton.Uid.t
        ; source_length: int
        ; target_length: int
      }
    | Carbon_copy of { depth: int; source: Carton.Uid.t; bstr: De.bigstring }

  let of_delta ~depth ~source ~src_len:source_length ~dst_len:target_length
      hunks =
    Patch { hunks; depth; source; source_length; target_length }

  let of_copy ~depth ~source bstr = Carbon_copy { depth; source; bstr }

  let source = function
    | Patch { source; _ } -> source
    | Carbon_copy { source; _ } -> source

  let length_of_variable_length n =
    let rec go r = function 0 -> r | n -> go (succ r) (n lsr 7) in
    go 1 (n lsr 7)

  let cmd off len =
    let cmd = ref 0 in
    if off land 0x000000ff <> 0 then cmd := !cmd lor 0x01;
    if off land 0x0000ff00 <> 0 then cmd := !cmd lor 0x02;
    if off land 0x00ff0000 <> 0 then cmd := !cmd lor 0x04;
    if off land 0x7f000000 <> 0 then cmd := !cmd lor 0x08;
    if len land 0x0000ff <> 0 then cmd := !cmd lor 0x10;
    if len land 0x00ff00 <> 0 then cmd := !cmd lor 0x20;
    if len land 0xff0000 <> 0 then cmd := !cmd lor 0x40;
    !cmd
  [@@inline]

  let length_of_copy_code ~off ~len =
    let required =
      let a = [| 0; 1; 1; 2; 1; 2; 2; 3; 1; 2; 2; 3; 2; 3; 3; 4 |] in
      fun x -> a.(x land 0xf) + a.(x lsr 4)
    in
    let cmd = cmd off len in
    required cmd

  let length ~source ~target hunks =
    let res =
      ref (length_of_variable_length source + length_of_variable_length target)
    in
    let fn = function
      | Duff.Insert (_, len) -> res := !res + 1 + len
      | Duff.Copy (off, len) -> res := !res + 1 + length_of_copy_code ~off ~len
    in
    List.iter fn hunks; !res

  let length = function
    | Patch { hunks; source_length; target_length; _ } ->
        length ~source:source_length ~target:target_length hunks
    | Carbon_copy { bstr; _ } -> De.bigstring_length bstr

  let pp ppf _t = Format.fprintf ppf "#patch"
end

module Target = struct
  type 'meta t = { mutable patch: Patch.t option; entry: 'meta Entry.t }

  let kind { entry; _ } = Entry.kind entry
  let length { entry; _ } = Entry.length entry
  let make ?patch entry = { patch; entry }

  let diff t ~source ~target =
    if
      Source.kind source <> kind t
      || Carton.Value.kind target <> kind t
      || Carton.Value.length target <> length t
      || Source.depth source >= 50
    then invalid_arg "Cartonnage.Target.diff";
    let source_length = Source.length source in
    let target_length = Carton.Value.length target in
    let index = Source.index source in
    let depth = Source.depth source + 1 in
    Log.debug (fun m ->
        m "Calculating diff between %a and %a (depth: %d)" Carton.Uid.pp
          (Source.uid source) Carton.Uid.pp (Entry.uid t.entry) depth);
    let hunks =
      let source = Source.bigstring source in
      let target =
        let bstr = Carton.Value.bigstring target in
        Bigarray.Array1.sub bstr 0 target_length
      in
      Duff.delta index ~source ~target
    in
    let source = Source.uid source in
    let patch =
      Patch.Patch { hunks; depth; source; source_length; target_length }
    in
    t.patch <- Some patch

  let uid { entry; _ } = Entry.uid entry
  let length { entry; _ } = Entry.length entry
  let patch { patch; _ } = patch
  let meta { entry; _ } = Entry.meta entry

  let depth { patch; _ } =
    match patch with
    | None -> 1
    | Some (Patch.Patch { depth; _ }) -> depth
    | Some (Patch.Carbon_copy { depth; _ }) -> depth

  let to_source t ~target =
    if Carton.Value.kind target <> kind t then
      invalid_arg "Cartonnage.Target.to_source: bad kind";
    if Carton.Value.length target <> length t then
      invalid_arg "Cartonnage.Target.to_source: bad length";
    let bstr = Carton.Value.bigstring target in
    let bstr = Bigarray.Array1.sub bstr 0 (Carton.Value.length target) in
    let index = Duff.make bstr in
    { Source.index; entry= t.entry; depth= depth t; value= target }
end

type buffers = {
    o: De.bigstring
  ; i: De.bigstring
  ; q: De.Queue.t
  ; w: De.Lz77.window
}

module Encoder = struct
  open Carton

  type encoder =
    | H of Zh.N.encoder
    | Z of Zl.Def.encoder
    | R of {
          src: De.bigstring
        ; src_off: int
        ; src_len: int
        ; dst_off: int
        ; dst_len: int
      }

  let bstr_length = Bigarray.Array1.dim

  let rec encode_zlib ~o encoder =
    match Zl.Def.encode encoder with
    | `Await encoder ->
        encode_zlib ~o (Zl.Def.src encoder De.bigstring_empty 0 0)
    | `Flush encoder ->
        let len = bstr_length o - Zl.Def.dst_rem encoder in
        `Flush (encoder, len)
    | `End encoder ->
        let len = bstr_length o - Zl.Def.dst_rem encoder in
        if len > 0 then `Flush (encoder, len) else `End

  let encode_hunk ~o encoder =
    match Zh.N.encode encoder with
    | `Flush encoder ->
        let len = bstr_length o - Zh.N.dst_rem encoder in
        `Flush (encoder, len)
    | `End -> `End

  let encode ~o = function
    | Z encoder -> begin
        match encode_zlib ~o encoder with
        | `Flush (encoder, len) -> `Flush (Z encoder, len)
        | `End -> `End
      end
    | H encoder -> begin
        match encode_hunk ~o encoder with
        | `Flush (encoder, len) -> `Flush (H encoder, len)
        | `End -> `End
      end
    | R { src_len= 0; _ } -> `End
    | R { src; src_off; src_len; dst_off; dst_len } ->
        let len = Int.min src_len dst_len in
        Cachet.memcpy src ~src_off o ~dst_off ~len;
        let state =
          R
            {
              src
            ; src_off= src_off + len
            ; src_len= src_len - len
            ; dst_off= dst_off + len
            ; dst_len= dst_len - len
            }
        in
        `Flush (state, dst_off + len)

  let dst encoder s j l =
    match encoder with
    | Z encoder ->
        let encoder = Zl.Def.dst encoder s j l in
        Z encoder
    | H encoder ->
        let encoder = Zh.N.dst encoder s j l in
        H encoder
    | R { src; src_off; src_len; _ } ->
        R { src; src_off; src_len; dst_off= j; dst_len= l }

  let encoder ?(level = 4) ~buffers entry target =
    let bstr = Carton.Value.bigstring target in
    let target_length = Carton.Value.length target in
    let bstr = Bigarray.Array1.sub bstr 0 target_length in
    match Target.patch entry with
    | Some (Patch.Patch { hunks; source_length= source; _ }) ->
        let { i; q; w; _ } = buffers in
        let encoder = Zh.N.encoder ~level ~i ~q ~w ~source bstr `Manual hunks in
        H encoder
    | Some (Patch.Carbon_copy { bstr= src; _ }) ->
        Log.debug (fun m ->
            m "Carbon copy of %a" Carton.Uid.pp (Target.uid entry));
        let src_off = 0
        and src_len = De.bigstring_length src
        and dst_off = 0
        and dst_len = 0 in
        R { src; src_off; src_len; dst_off; dst_len }
    | None ->
        let { q; w; _ } = buffers in
        let encoder = Zl.Def.encoder `Manual `Manual ~q ~w ~level in
        let encoder = Zl.Def.src encoder bstr 0 target_length in
        Z encoder
end

external bigstring_set_uint8 : De.bigstring -> int -> int -> unit
  = "%caml_ba_set_1"

external bigstring_set_int32_ne : De.bigstring -> int -> int32 -> unit
  = "%caml_bigstring_set32"

let bigstring_blit_from_bytes src ~src_off dst ~dst_off ~len =
  let len0 = len land 3 in
  let len1 = len lsr 2 in
  for i = 0 to len1 - 1 do
    let i = i * 4 in
    let v = Bytes.get_int32_ne src (src_off + i) in
    bigstring_set_int32_ne dst (dst_off + i) v
  done;
  for i = 0 to len0 - 1 do
    let i = (len1 * 4) + i in
    let v = Bytes.get_uint8 src (src_off + i) in
    bigstring_set_uint8 dst (dst_off + i) v
  done

let bigstring_blit_from_string src ~src_off dst ~dst_off ~len =
  bigstring_blit_from_bytes
    (Bytes.unsafe_of_string src)
    ~src_off dst ~dst_off ~len

let encode_header ~output kind length =
  if length < 0 then
    invalid_arg "Cartonnage.encode_header: length must be positive";
  let c = ref ((kind lsl 4) lor (length land 15)) in
  let l = ref (length asr 4) in
  let p = ref 0 in
  let n = ref 1 in
  while !l != 0 do
    bigstring_set_uint8 output !p (!c lor 0x80 land 0xff);
    incr p;
    c := !l land 0x7f;
    l := !l asr 7;
    incr n
  done;
  bigstring_set_uint8 output !p !c;
  !n

module Kind = struct
  include Carton.Kind

  let to_int = function `A -> 0b001 | `B -> 0b010 | `C -> 0b011 | `D -> 0b100
end

type where = Carton.Uid.t -> int option

let encode ?level ~buffers ~where entry ~target ~cursor =
  match Target.patch entry with
  | None ->
      let kind = Kind.to_int (Target.kind entry) in
      let length = Target.length entry in
      let off = encode_header ~output:buffers.o kind length in
      let encoder = Encoder.encoder ?level ~buffers entry target in
      let len = Encoder.bstr_length buffers.o - off in
      (off, Encoder.dst encoder buffers.o off len)
  | Some (Patch.(Patch { source; _ } | Carbon_copy { source; _ }) as patch) ->
    begin
      Log.debug (fun m -> m "search %a as a source" Carton.Uid.pp source);
      match where source with
      | Some offset ->
          let kind = 0b110 in
          let length = Patch.length patch in
          let dst_off = encode_header ~output:buffers.o kind length in
          let buf = Bytes.create 10 in
          let p = ref (10 - 1) in
          let n = ref (cursor - offset) in
          Log.debug (fun m ->
              m "encode a new OBJ_OFS with %08x (cursor:%08x, offset:%08x)"
                (cursor - offset) cursor offset);
          Bytes.set_uint8 buf !p (!n land 127);
          while !n asr 7 <> 0 do
            n := !n asr 7;
            decr p;
            Bytes.set_uint8 buf !p (128 lor ((!n - 1) land 127));
            decr n
          done;
          bigstring_blit_from_bytes buf ~src_off:!p buffers.o ~dst_off
            ~len:(10 - !p);
          let encoder = Encoder.encoder ?level ~buffers entry target in
          let off = dst_off + (10 - !p) in
          let len = Encoder.bstr_length buffers.o - off in
          (off, Encoder.dst encoder buffers.o off len)
      | None ->
          let kind = 0b111 in
          let length = Patch.length patch in
          let dst_off = encode_header ~output:buffers.o kind length in
          bigstring_blit_from_string
            (source :> string)
            ~src_off:0 buffers.o ~dst_off
            ~len:(String.length (source :> string));
          let encoder = Encoder.encoder ?level ~buffers entry target in
          let off = dst_off + String.length (source :> string) in
          let len = Encoder.bstr_length buffers.o - off in
          (off, Encoder.dst encoder buffers.o off len)
    end
