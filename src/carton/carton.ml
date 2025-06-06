module H = H
module Zh = Zh

let src = Logs.Src.create "carton"

module Log = (val Logs.src_log src : Logs.LOG)

let invalid_argf fmt = Format.kasprintf invalid_arg fmt
let failwithf fmt = Format.kasprintf failwith fmt

module Kind = struct
  type t = [ `A | `B | `C | `D ]

  let pp ppf = function
    | `A -> Format.pp_print_string ppf "a"
    | `B -> Format.pp_print_string ppf "b"
    | `C -> Format.pp_print_string ppf "c"
    | `D -> Format.pp_print_string ppf "d"

  let compare a b = Stdlib.compare a b
  let equal a b = a = b
  let to_int = function `A -> 0 | `B -> 1 | `C -> 2 | `D -> 3
end

module Size = struct
  type t = int

  let of_int_exn value =
    if value < 0 then invalid_arg "Carton.Dec.Size.of_int_exn";
    value

  let to_int x = x
  let zero = 0
  let compare = Int.compare
  let equal = Int.equal
  let max = Int.max
end

module Uid = struct
  type t = string

  let unsafe_of_string x = x
  let compare = String.compare
  let equal = String.equal

  let pp ppf str =
    for i = 0 to String.length str - 1 do
      Format.fprintf ppf "%02x" (Char.code str.[i])
    done
end

module First_pass = struct
  type 'ctx hash = {
      feed_bytes: bytes -> off:int -> len:int -> 'ctx -> 'ctx
    ; feed_bigstring: Bstr.t -> 'ctx -> 'ctx
    ; serialize: 'ctx -> string
    ; length: int
  }

  type digest = Digest : 'ctx hash * 'ctx -> digest
  type src = [ `String of string | `Manual ]

  type 'ctx identify = {
      init: Kind.t -> Size.t -> 'ctx
    ; feed: Bstr.t -> 'ctx -> 'ctx
    ; serialize: 'ctx -> Uid.t
  }

  type kind =
    | Base of Kind.t
    | Ofs of { sub: int; source: int; target: int }
    | Ref of { ptr: string; source: int; target: int }

  let digest bstr ?(off = 0) ?len
      (Digest (({ feed_bigstring; _ } as hash), ctx)) =
    let default = Bigarray.Array1.dim bstr - off in
    let len = Option.value ~default len in
    let bstr = Bigarray.Array1.sub bstr off len in
    Digest (hash, feed_bigstring bstr ctx)

  let serialize (Digest ({ serialize; _ }, ctx)) = serialize ctx
  let length_of_hash (Digest ({ length; _ }, _)) = length

  [@@@warning "-30"]

  type decoder = {
      src: src
    ; input: Bstr.t
    ; input_pos: int
    ; input_len: int
    ; number_of_objects: int
    ; (* number of objects *)
      counter_of_objects: int
    ; (* counter of objects *)
      version: int
    ; (* version of PACK file *)
      consumed: int64
    ; (* how many bytes consumed *)
      state: state
    ; output: Bstr.t
    ; tmp: Bstr.t
    ; tmp_len: int
    ; tmp_need: int
    ; tmp_peek: int
    ; zlib: Zl.Inf.decoder
    ; ref_length: int
    ; digest: digest
    ; k: decoder -> decode
  }

  and state = Header | Entry_header | Inflate of entry | Entry of entry | Hash

  and decode =
    [ `Await of decoder
    | `Peek of decoder
    | `Inflate of string * decoder
    | `Entry of entry * decoder
    | `End of string
    | `Malformed of string ]

  and entry = {
      offset: int
    ; kind: kind
    ; size: int
    ; consumed: int
    ; crc: Optint.t
    ; number: int
  }

  open Checkseum

  let digest_decoder decoder ~len =
    let off = decoder.input_pos in
    let digest = digest decoder.input ~off ~len decoder.digest in
    { decoder with digest }

  let crc_decoder decoder ~len crc =
    let off = decoder.input_pos in
    Crc32.digest_bigstring decoder.input off len crc

  let bstr_length = Bigarray.Array1.dim
  let ( +! ) = Int64.add
  let ( -! ) = Int64.sub

  let with_source source entry =
    match entry.kind with
    | Ofs { sub; target; _ } -> { entry with kind= Ofs { sub; source; target } }
    | Ref { ptr; target; _ } -> { entry with kind= Ref { ptr; source; target } }
    | _ -> entry

  let with_target target entry =
    match entry.kind with
    | Ofs { sub; source; _ } -> { entry with kind= Ofs { sub; source; target } }
    | Ref { ptr; source; _ } -> { entry with kind= Ref { ptr; source; target } }
    | _ -> entry

  let source entry =
    match entry.kind with
    | Ofs { source; _ } | Ref { source; _ } -> source
    | _ -> assert false

  let target entry =
    match entry.kind with
    | Ofs { target; _ } | Ref { target; _ } -> target
    | _ -> assert false

  let is_first entry =
    let source = source entry in
    let target = target entry in
    source == -1 && target == -1

  let number_of_objects { number_of_objects; _ } = number_of_objects
  let version { version; _ } = version
  let counter_of_objects { counter_of_objects; _ } = counter_of_objects
  let is_inflate = function Inflate _ -> true | _ -> false

  let kind decoder =
    match decoder.state with
    | Inflate { kind= Base kind; size; _ } -> Some (kind, size)
    | _ -> None

  let src_rem decoder = decoder.input_len - decoder.input_pos + 1

  let end_of_input decoder =
    { decoder with input= Bstr.empty; input_pos= 0; input_len= min_int }

  let malformedf fmt = Format.kasprintf (fun err -> `Malformed err) fmt
  let hash { digest; _ } = digest

  let zlib_src decoder =
    let input = decoder.input
    and off = decoder.input_pos
    and len = src_rem decoder in
    let zlib = Zl.Inf.src decoder.zlib input off len in
    { decoder with zlib }

  let src decoder src idx len =
    if idx < 0 || len < 0 || idx + len > bstr_length src then
      invalid_argf "First_pass.src: source out of bounds";
    if len == 0 then end_of_input decoder
    else
      let decoder =
        { decoder with input= src; input_pos= idx; input_len= idx + len - 1 }
      in
      if is_inflate decoder.state then zlib_src decoder else decoder

  let refill k decoder =
    match decoder.src with
    | `String _ -> k (end_of_input decoder)
    | `Manual -> `Await { decoder with k }

  let rec peek k decoder =
    match decoder.src with
    | `String _ -> malformedf "First_pass.peek: unexpected end of input"
    | `Manual ->
        let rem = src_rem decoder in
        if rem < decoder.tmp_peek then begin
          let src_off = decoder.input_pos in
          Cachet.memmove decoder.input ~src_off decoder.input ~dst_off:0
            ~len:rem;
          `Peek { decoder with k= peek k; input_pos= 0; input_len= rem - 1 }
        end
        else k decoder

  let tmp_need decoder n = { decoder with tmp_need= n; tmp_len= 0 }
  let tmp_peek decoder n = { decoder with tmp_peek= n }

  let rec tmp_fill k decoder =
    let blit decoder len =
      let src_off = decoder.input_pos and dst_off = decoder.tmp_len in
      Cachet.memcpy decoder.input ~src_off decoder.tmp ~dst_off ~len;
      {
        decoder with
        input_pos= decoder.input_pos + len
      ; consumed= decoder.consumed +! Int64.of_int len
      ; tmp_len= decoder.tmp_len + len
      }
    in
    let rem = src_rem decoder in
    if rem < 0 then malformedf "First_pass.fill: Unexpected end of input"
    else
      let need = decoder.tmp_need - decoder.tmp_len in
      (* XXX(dinosaure): in the [`Manual] case, [input_pos = 1] and [blit] will
         fail where offset with an empty buffer raises an exception. We protect
         it by [rem = 0] and directly ask to refill inputs. *)
      if rem = 0 then refill (tmp_fill k) decoder
      else if rem < need then
        let decoder = blit decoder rem in
        refill (tmp_fill k) decoder
      else
        let decoder = blit decoder need in
        k { decoder with tmp_need= 0 }

  let variable_length buf off top =
    let p = ref off in
    let i = ref 0 in
    let len = ref 0 in
    while
      let cmd = Bstr.get_uint8 buf !p in
      incr p;
      len := !len lor ((cmd land 0x7f) lsl !i);
      i := !i + 7;
      cmd land 0x80 != 0 && !p <= top
    do
      ()
    done;
    (!p - off, !len)

  let _max_int31 = 2147483647l (* (1 << 31) - 1 *)
  let peek_15 k decoder = peek k (tmp_peek decoder 15)

  let peek_uid k decoder =
    peek k
      (tmp_peek decoder
         (decoder.ref_length
        +
        (* zlib *)
        2))

  let rec ref_header crc offset size decoder =
    let anchor = decoder.input_pos in
    let off = anchor and len = decoder.ref_length in
    let ptr = Bstr.sub_string decoder.input ~off ~len in
    let crc = crc_decoder decoder ~len:decoder.ref_length crc in
    let decoder = digest_decoder decoder ~len:decoder.ref_length in
    let decoder = { decoder with input_pos= anchor + decoder.ref_length } in
    let decoder = { decoder with zlib= Zl.Inf.reset decoder.zlib } in
    let decoder = zlib_src decoder in
    let entry =
      {
        offset
      ; kind= Ref { ptr; source= -1; target= -1 }
      ; size
      ; consumed= 0
      ; crc
      ; number= decoder.counter_of_objects
      }
    in
    (decode [@tailcall])
      {
        decoder with
        consumed= decoder.consumed +! Int64.of_int decoder.ref_length
      ; counter_of_objects= succ decoder.counter_of_objects
      ; state= Inflate entry
      ; k= decode
      }

  and ofs_header crc offset size decoder =
    let p = ref decoder.input_pos in
    let c = ref (Bstr.get_uint8 decoder.input !p) in
    incr p;
    let base_offset = ref (!c land 127) in
    while !c land 128 != 0 do
      incr base_offset;
      c := Bstr.get_uint8 decoder.input !p;
      incr p;
      base_offset := (!base_offset lsl 7) + (!c land 127)
    done;
    let len = !p - decoder.input_pos in
    let crc = crc_decoder decoder ~len crc in
    let decoder = digest_decoder decoder ~len in
    let decoder = { decoder with input_pos= !p } in
    let decoder = { decoder with zlib= Zl.Inf.reset decoder.zlib } in
    let decoder = zlib_src decoder in
    let entry =
      {
        offset
      ; kind= Ofs { sub= !base_offset; source= -1; target= -1 }
      ; size
      ; consumed= 0
      ; crc
      ; number= decoder.counter_of_objects
      }
    in
    (decode [@tailcall])
      {
        decoder with
        consumed= decoder.consumed +! Int64.of_int len
      ; counter_of_objects= succ decoder.counter_of_objects
      ; state= Inflate entry
      ; k= decode
      }

  and entry_header decoder =
    Log.debug (fun m ->
        m "object %d/%d" decoder.counter_of_objects decoder.number_of_objects);
    let p = ref decoder.input_pos in
    let c = ref (Bstr.get_uint8 decoder.input !p) in
    incr p;
    let kind = (!c asr 4) land 7 in
    let size = ref (!c land 15) in
    let shft = ref 4 in
    while !c land 0x80 != 0 do
      c := Bstr.get_uint8 decoder.input !p;
      incr p;
      size := !size + ((!c land 0x7f) lsl !shft);
      shft := !shft + 7
    done;
    let len = !p - decoder.input_pos in
    let crc = Crc32.default in
    let crc = crc_decoder decoder ~len crc in
    let decoder = digest_decoder decoder ~len in
    match kind with
    | 0b000 | 0b101 -> malformedf "Carton.Dec.First_pass: invalid type"
    | (0b001 | 0b010 | 0b011 | 0b100) as kind ->
        let decoder = { decoder with input_pos= decoder.input_pos + len } in
        let decoder = { decoder with zlib= Zl.Inf.reset decoder.zlib } in
        let decoder = zlib_src decoder in
        let kind =
          match kind with
          | 0b001 -> `A
          | 0b010 -> `B
          | 0b011 -> `C
          | 0b100 -> `D
          | _ -> assert false
        in
        let entry =
          {
            offset= Int64.to_int decoder.consumed
          ; kind= Base kind
          ; size= !size
          ; consumed= 0
          ; crc
          ; number= decoder.counter_of_objects
          }
        in
        decode
          {
            decoder with
            consumed= decoder.consumed +! Int64.of_int len
          ; counter_of_objects= succ decoder.counter_of_objects
          ; state= Inflate entry
          ; k= decode
          }
    | 0b110 ->
        let offset = Int64.to_int decoder.consumed in
        let k = ofs_header crc offset !size in
        peek_15 k
          {
            decoder with
            input_pos= !p
          ; consumed= decoder.consumed +! Int64.of_int len
          }
    | 0b111 ->
        let offset = Int64.to_int decoder.consumed in
        let k = ref_header crc offset !size in
        peek_uid k
          {
            decoder with
            input_pos= !p
          ; consumed= decoder.consumed +! Int64.of_int len
          }
    | _ -> assert false (* NOTE(dinosaure): impossible due to our mask. *)

  and refill_12 k t =
    if src_rem t >= 12 then
      k t.input t.input_pos
        { t with input_pos= t.input_pos + 12; consumed= t.consumed +! 12L }
    else tmp_fill (k t.tmp 0) (tmp_need t 12)

  and refill_uid k t =
    let required = length_of_hash t.digest in
    if src_rem t >= required then
      k t.input t.input_pos
        {
          t with
          input_pos= t.input_pos + required
        ; consumed= t.consumed +! Int64.of_int required
        }
    else tmp_fill (k t.tmp 0) (tmp_need t required)

  and verify_pack_header buf off decoder =
    let version = Bstr.get_int32_be buf (off + 4) in
    let number_of_objects = Bstr.get_int32_be buf (off + 8) in
    if version <> 2l then
      invalid_argf "Invalid version of PACK file (%04lx)" version;
    (* XXX(dinosaure): or [malformedf]? *)
    if number_of_objects > _max_int31 && Sys.word_size = 32 then
      failwith "Too huge PACK file for a 32-bits machine";
    let digest = digest buf ~off ~len:12 decoder.digest in
    if decoder.counter_of_objects == Int32.to_int number_of_objects then
      decode
        {
          decoder with
          version= Int32.to_int version
        ; number_of_objects= Int32.to_int number_of_objects
        ; state= Hash
        ; k= decode
        ; digest
        }
    else
      decode
        {
          decoder with
          version= Int32.to_int version
        ; number_of_objects= Int32.to_int number_of_objects
        ; state= Entry_header
        ; k= decode
        ; digest
        }

  and verify_signature buf off decoder =
    let have = serialize decoder.digest in
    let expect = Bstr.sub_string buf ~off ~len:(String.length have) in
    if String.equal expect have then `End have
    else
      malformedf "Invalid hash (%s != %s)" (Ohex.encode expect)
        (Ohex.encode have)

  and decode decoder =
    match decoder.state with
    | Header -> refill_12 verify_pack_header decoder
    | Entry_header ->
        (* TODO(dinosaure): we need something more robust than [15] where when
           it's not enough to have the ofs-header and the zlib-header,
           [decompress] returns an error - because we fill at the beginning the
           input buffer with [0] (then, we reach end-of-input). *)
        peek_15 entry_header decoder
    | Entry entry ->
        if decoder.counter_of_objects == decoder.number_of_objects then
          `Entry (entry, { decoder with state= Hash })
        else `Entry (entry, { decoder with state= Entry_header })
    | Inflate ({ kind= Base _; crc; _ } as entry) -> begin
        Log.debug (fun m -> m "inflate base object");
        match Zl.Inf.decode decoder.zlib with
        | `Await zlib ->
            let len = src_rem decoder - Zl.Inf.src_rem zlib in
            let crc = crc_decoder decoder ~len crc in
            let decoder = digest_decoder decoder ~len in
            refill decode
              {
                decoder with
                zlib
              ; input_pos= decoder.input_pos + len
              ; consumed= decoder.consumed +! Int64.of_int len
              ; state= Inflate { entry with crc }
              }
        | `Flush zlib ->
            let olen = Bstr.length decoder.output - Zl.Inf.dst_rem zlib in
            let str = Bstr.sub_string decoder.output ~off:0 ~len:olen in
            let ilen = src_rem decoder - Zl.Inf.src_rem zlib in
            let crc = crc_decoder decoder ~len:ilen crc in
            let decoder = digest_decoder decoder ~len:ilen in
            let zlib = Zl.Inf.flush zlib in
            let decoder =
              {
                decoder with
                zlib
              ; input_pos= decoder.input_pos + ilen
              ; consumed= decoder.consumed +! Int64.of_int ilen
              ; state= Inflate { entry with crc }
              }
            in
            `Inflate (str, decoder)
        | `Malformed err -> malformedf "Pack.decode.inflate (base): %s" err
        | `End zlib ->
            let olen = Bstr.length decoder.output - Zl.Inf.dst_rem zlib in
            let str = Bstr.sub_string decoder.output ~off:0 ~len:olen in
            let ilen = src_rem decoder - Zl.Inf.src_rem zlib in
            let crc = crc_decoder decoder ~len:ilen crc in
            let decoder = digest_decoder decoder ~len:ilen in
            let zlib = Zl.Inf.reset zlib in
            let decoder =
              {
                decoder with
                input_pos= decoder.input_pos + ilen
              ; consumed= decoder.consumed +! Int64.of_int ilen
              ; zlib
              ; k= decode
              }
            in
            let consumed = Int64.(decoder.consumed -! of_int entry.offset) in
            let consumed = Int64.to_int consumed in
            let entry = { entry with consumed; crc } in
            let state = Entry entry in
            `Inflate (str, { decoder with state })
      end
    | Inflate ({ kind= Ofs _ | Ref _; crc; _ } as entry) -> begin
        match Zl.Inf.decode decoder.zlib with
        | `Await zlib ->
            let len = src_rem decoder - Zl.Inf.src_rem zlib in
            let crc = crc_decoder decoder ~len crc in
            let decoder = digest_decoder decoder ~len in
            refill decode
              {
                decoder with
                zlib
              ; input_pos= decoder.input_pos + len
              ; consumed= decoder.consumed +! Int64.of_int len
              ; state= Inflate { entry with crc }
              }
        | `Flush zlib ->
            let olen = Bstr.length decoder.output - Zl.Inf.dst_rem zlib in
            let str = Bstr.sub_string decoder.output ~off:0 ~len:olen in
            let ilen = src_rem decoder - Zl.Inf.src_rem zlib in
            let crc = crc_decoder decoder ~len:ilen crc in
            let entry =
              if is_first entry then
                let x, src_len = variable_length decoder.output 0 olen in
                let _, dst_len = variable_length decoder.output x (olen - x) in
                let entry = with_source src_len entry in
                let entry = with_target dst_len entry in
                { entry with crc }
              else { entry with crc }
            in
            let decoder = digest_decoder decoder ~len:ilen in
            let zlib = Zl.Inf.flush zlib in
            let decoder =
              {
                decoder with
                zlib
              ; input_pos= decoder.input_pos + ilen
              ; consumed= decoder.consumed +! Int64.of_int ilen
              ; state= Inflate entry
              }
            in
            `Inflate (str, decoder)
        | `Malformed err -> malformedf "Pack.decode.inflate (delta): %s" err
        | `End zlib ->
            let olen = Bstr.length decoder.output - Zl.Inf.dst_rem zlib in
            let str = Bstr.sub_string decoder.output ~off:0 ~len:olen in
            let ilen = src_rem decoder - Zl.Inf.src_rem zlib in
            let crc = crc_decoder decoder ~len:ilen crc in
            let entry =
              if is_first entry then
                let x, src_len = variable_length decoder.output 0 olen in
                let _, dst_len = variable_length decoder.output x (olen - x) in
                let entry = with_source src_len entry in
                let entry = with_target dst_len entry in
                { entry with crc }
              else { entry with crc }
            in
            let zlib = Zl.Inf.reset zlib in
            let decoder = digest_decoder decoder ~len:ilen in
            let decoder =
              {
                decoder with
                input_pos= decoder.input_pos + ilen
              ; consumed= decoder.consumed +! Int64.of_int ilen
              ; zlib
              ; k= decode
              }
            in
            let consumed = Int64.(decoder.consumed -! of_int entry.offset) in
            let consumed = Int64.to_int consumed in
            let entry = { entry with consumed } in
            let state = Entry entry in
            `Inflate (str, { decoder with state })
      end
    | Hash -> refill_uid verify_signature decoder

  let decoder ~output ~allocate ~ref_length ~digest src =
    let input, input_pos, input_len =
      match src with
      | `Manual -> (Bstr.empty, 1, 0)
      | `String str ->
          let input = Bstr.of_string str in
          (input, 0, String.length str - 1)
    in
    {
      src
    ; input
    ; input_pos
    ; input_len
    ; number_of_objects= 0
    ; counter_of_objects= 0
    ; version= 0
    ; consumed= 0L
    ; output
    ; state= Header
    ; tmp= Bstr.create (Int.min 20 (length_of_hash digest))
    ; tmp_len= 0
    ; tmp_need= 0
    ; tmp_peek= 0
    ; zlib= Zl.Inf.decoder `Manual ~o:output ~allocate
    ; ref_length
    ; k= decode
    ; digest
    }

  let decode decoder = decoder.k decoder

  let of_seq ~output ~allocate ~ref_length ~digest seq =
    let input = Bstr.create De.io_buffer_size in
    let first = ref true in
    let rec go decoder seq (str, src_off, src_len) () =
      match decode decoder with
      | `Await decoder ->
          if src_len == 0 then
            match Seq.uncons seq with
            | Some (str, seq) ->
                let len = Int.min (bstr_length input) (String.length str) in
                Bstr.blit_from_string str ~src_off:0 input ~dst_off:0 ~len;
                let decoder = src decoder input 0 len in
                go decoder seq (str, len, String.length str - len) ()
            | None ->
                let decoder = src decoder Bstr.empty 0 0 in
                go decoder seq (String.empty, 0, 0) ()
          else begin
            let len = Int.min (bstr_length input) src_len in
            Bstr.blit_from_string str ~src_off input ~dst_off:0 ~len;
            let decoder = src decoder input 0 len in
            go decoder seq (str, src_off + len, src_len - len) ()
          end
      | `Peek decoder ->
          let dst_off = src_rem decoder in
          if src_len == 0 then
            match Seq.uncons seq with
            | Some (str, seq) ->
                let len =
                  Int.min (bstr_length input - dst_off) (String.length str)
                in
                Bstr.blit_from_string str ~src_off:0 input ~dst_off ~len;
                let decoder = src decoder input 0 (dst_off + len) in
                go decoder seq (str, len, String.length str - len) ()
            | None ->
                let decoder = src decoder Bstr.empty 0 0 in
                go decoder seq (String.empty, 0, 0) ()
          else begin
            let len = Int.min (bstr_length input - dst_off) src_len in
            Bstr.blit_from_string str ~src_off input ~dst_off ~len;
            let decoder = src decoder input 0 (dst_off + len) in
            go decoder seq (str, src_off + len, src_len - len) ()
          end
      | `Inflate (payload, decoder) ->
          let next = go decoder seq (str, src_off, src_len) in
          let kind = kind decoder in
          Log.debug (fun m ->
              m "emit inflated payload (base? %b)" (Option.is_some kind));
          Seq.Cons (`Inflate (kind, payload), next)
      | `Entry (entry, decoder) ->
          let next = go decoder seq (str, src_off, src_len) in
          begin
            match !first with
            | true ->
                first := false;
                let n = number_of_objects decoder in
                let next () = Seq.Cons (`Entry entry, next) in
                Seq.Cons (`Number n, next)
            | false -> Seq.Cons (`Entry entry, next)
          end
      | `Malformed err -> failwith err
      | `End hash -> Seq.Cons (`Hash hash, Fun.const Seq.Nil)
    in
    let decoder = decoder ~output ~allocate ~ref_length ~digest `Manual in
    fun () -> match Seq.uncons seq with
    | Some (str, seq) ->
        let len = Int.min (bstr_length input) (String.length str) in
        Bstr.blit_from_string str ~src_off:0 input ~dst_off:0 ~len;
        let decoder = src decoder input 0 len in
        go decoder seq (str, len, String.length str - len) ()
    | None ->
        Log.debug (fun m -> m "No PACK file are given");
        Seq.Nil
end

let _max_depth = 60

module Blob = struct
  type t = { raw0: Bstr.t; raw1: Bstr.t; flip: bool }

  let make ~size =
    let raw0 = Bstr.create size in
    let raw1 = Bstr.create size in
    { raw0; raw1; flip= false }

  let size { raw0; _ } = Bigarray.Array1.dim raw0
  let source { raw0; raw1; flip } = if flip then raw1 else raw0
  let payload { raw0; raw1; flip } = if flip then raw0 else raw1
  let flip t = { t with flip= not t.flip }

  let of_string str =
    let len = String.length str in
    let raw0 = Bstr.create len in
    Bstr.blit_from_string str ~src_off:0 raw0 ~dst_off:0 ~len;
    { raw0; raw1= Bstr.empty; flip= true }

  let with_source t ~source =
    if t.flip then { t with raw1= source } else { t with raw0= source }
end

module Value = struct
  type t = { kind: Kind.t; blob: Blob.t; len: int; depth: int }

  let kind { kind; _ } = kind
  let bigstring { blob; _ } = Blob.payload blob
  let length { len; _ } = len
  let depth { depth; _ } = depth
  let blob { blob; _ } = blob
  let flip value = { value with blob= Blob.flip value.blob }
  let source { blob; _ } = Blob.source blob
  let with_source t ~source = { t with blob= Blob.with_source ~source t.blob }

  let make ~kind ?(depth = 1) bstr =
    let len = Bigarray.Array1.dim bstr in
    let blob = { Blob.raw0= bstr; raw1= Bstr.empty; flip= true } in
    { kind; blob; len; depth }

  let of_blob ~kind ~length:len ?(depth = 1) blob = { kind; blob; len; depth }

  let of_string ~kind ?(depth = 1) str =
    let len = String.length str in
    let blob = Blob.of_string str in
    { kind; blob; len; depth }

  let string { blob; len; _ } =
    let bstr = Blob.payload blob in
    Bstr.sub_string bstr ~off:0 ~len

  let pp ppf t =
    Format.fprintf ppf "{ @[<hov>kind= %a;@ len= %d;@ depth= %d;@] }" Kind.pp
      t.kind t.len t.depth
end

type location = Local of int | Extern of Kind.t * Bstr.t

type 'fd t = {
    cache: 'fd Cachet.t
  ; where: string -> location
  ; ref_length: int
  ; tmp: Bstr.t
  ; allocate: int -> Zl.window
}

let fd { cache; _ } = Cachet.fd cache
let cache { cache; _ } = cache
let allocate { allocate; _ } = allocate
let tmp { tmp; _ } = tmp
let ref_length { ref_length; _ } = ref_length
let with_index t where = { t with where }

let make ?pagesize ?cachesize ~map fd ~z:tmp ~allocate ~ref_length where =
  {
    cache= Cachet.make ?pagesize ?cachesize ~map fd
  ; where
  ; ref_length
  ; tmp
  ; allocate
  }

let of_cache cache ~z:tmp ~allocate ~ref_length where =
  { cache; where; ref_length; tmp; allocate }

let copy t =
  {
    cache= Cachet.copy t.cache
  ; where= t.where
  ; ref_length= t.ref_length
  ; tmp= Bstr.copy t.tmp
  ; allocate= t.allocate
  }

let size_of_delta t ~cursor size =
  let rec go slice decoder =
    match Zh.M.decode decoder with
    | `End _ -> assert false
    | `Malformed err -> failwith err
    | `Header (src_len, dst_len, _) -> Int.max size (Int.max src_len dst_len)
    | `Await decoder -> begin
        match Cachet.next t.cache slice with
        | None ->
            let decoder = Zh.M.src decoder Bstr.empty 0 0 in
            (go [@tailcall]) slice decoder
        | Some ({ payload; length; _ } as slice) ->
            let decoder = Zh.M.src decoder (payload :> Bstr.t) 0 length in
            (go [@tailcall]) slice decoder
      end
  in
  let decoder = Zh.M.decoder ~o:t.tmp ~allocate:t.allocate `Manual in
  Log.debug (fun m -> m "load %08x" cursor);
  match Cachet.load t.cache cursor with
  | Some ({ offset; payload; length } as slice) ->
      let off = cursor - offset in
      let len = length - off in
      let decoder = Zh.M.src decoder (payload :> Bstr.t) off len in
      go slice decoder
  | None -> raise (Cachet.Out_of_bounds cursor)

let header_of_ref_delta t ~cursor =
  Cachet.get_string t.cache ~len:t.ref_length cursor

let header_of_ofs_delta t ~cursor =
  Log.debug (fun m -> m "decode ofs header at %08x" cursor);
  let str = Cachet.get_string t.cache ~len:10 cursor in
  let p = ref 0 in
  let c = ref (String.get_uint8 str !p) in
  incr p;
  let rel_offset = ref (!c land 127) in
  while !c land 128 != 0 do
    incr rel_offset;
    c := String.get_uint8 str !p;
    incr p;
    rel_offset := (!rel_offset lsl 7) + (!c land 127)
  done;
  (!rel_offset, cursor + !p)

let header_of_entry t ~cursor =
  let str = Cachet.get_string t.cache ~len:10 cursor in
  let p = ref 0 in
  let c = ref (String.get_uint8 str 0) in
  incr p;
  let kind = (!c lsr 4) land 7 in
  let size = ref (!c land 15) in
  let shft = ref 4 in
  while !c land 0x80 != 0 do
    c := String.get_uint8 str !p;
    incr p;
    size := !size + ((!c land 0x7f) lsl !shft);
    shft := !shft + 7
  done;
  ((kind, !size), cursor + !p)

exception Cycle

module Visited = struct
  type t = { depth: int; path: int array }

  let empty = { depth= 0; path= Array.make _max_depth (-1) }

  let already_visited { path; depth } ~cursor =
    let exception Yes in
    try
      for i = 0 to depth - 1 do
        if path.(i) == cursor then raise_notrace Yes
      done;
      false
    with Yes -> true
end

exception Too_deep
exception Bad_type

let rec size_of_ref_delta t ?visited ~cursor size =
  let uid = header_of_ref_delta t ~cursor in
  let size = size_of_delta t ~cursor:(cursor + t.ref_length) size in
  (size_of_uid [@tailcall]) t ?visited ~uid size

and size_of_ofs_delta t ?visited ~anchor ~cursor size =
  let rel_offset, cursor' = header_of_ofs_delta t ~cursor in
  let size = size_of_delta t ~cursor:cursor' size in
  (size_of_offset [@tailcall]) t ?visited ~cursor:(anchor - rel_offset) size

and size_of_uid t ?visited ~uid size =
  match t.where uid with
  | Local cursor -> (size_of_offset [@tailcall]) t ?visited ~cursor size
  | Extern (_kind, bstr) -> Int.max size (Bstr.length bstr)

and size_of_offset t ?(visited = Visited.empty) ~cursor size =
  if Visited.already_visited visited ~cursor then raise Cycle;
  visited.path.(visited.depth) <- cursor;
  let visited = { visited with depth= succ visited.depth } in
  if visited.depth >= _max_depth then raise Too_deep;
  let (kind, size'), cursor' = header_of_entry t ~cursor in
  match kind with
  | 0b000 | 0b101 -> raise Bad_type
  | 0b001 | 0b010 | 0b011 | 0b100 -> Int.max size size'
  | 0b110 ->
      (size_of_ofs_delta [@tailcall]) t ~visited ~anchor:cursor ~cursor:cursor'
        (Int.max size size')
  | 0b111 ->
      (size_of_ref_delta [@tailcall]) t ~visited ~cursor:cursor'
        (Int.max size size')
  | _ -> assert false

let map t ~cursor ~consumed =
  let (kind, _), cursor' = header_of_entry t ~cursor in
  match kind with
  | 0b000 | 0b101 -> raise Bad_type
  | 0b001 | 0b010 | 0b011 | 0b100 ->
      let size = consumed - (cursor' - cursor) in
      Cachet.map t.cache ~pos:cursor' size
  | 0b110 ->
      let _, cursor' = header_of_ofs_delta t ~cursor:cursor' in
      let size = consumed - (cursor' - cursor) in
      Cachet.map t.cache ~pos:cursor' size
  | 0b111 ->
      let size = consumed - (cursor' + t.ref_length - cursor) in
      Cachet.map t.cache ~pos:(cursor' + t.ref_length) size
  | _ -> assert false

let actual_size_of_offset : type fd. fd t -> cursor:int -> int =
 fun t ~cursor ->
  let (_, size), _ = header_of_entry t ~cursor in
  size

let uncompress t kind blob ~cursor =
  let anchor = cursor in
  let o = Blob.payload blob in
  let rec go ~real_length ~flushed slice decoder =
    match Zl.Inf.decode decoder with
    | `Malformed err -> failwithf "object <%08x>: %s" anchor err
    | `End decoder ->
        let len = Bigarray.Array1.dim o - Zl.Inf.dst_rem decoder in
        assert (flushed || ((not flushed) && len = 0));
        (* XXX(dinosaure): we gave a [o] buffer which is enough to store
           inflated data. At the end, [decoder] should not return more than one
           [`Flush]. A special case is when we inflate nothing: [`Flush] never
           appears and we reach [`End] directly, so [!p (still) = false and len
           (must) = 0]. *)
        { Value.kind; blob; len= real_length; depth= 1 }
    | `Flush decoder ->
        let real_length = Bigarray.Array1.dim o - Zl.Inf.dst_rem decoder in
        assert (not flushed);
        let decoder = Zl.Inf.flush decoder in
        (go [@tailcall]) ~real_length ~flushed:true slice decoder
    | `Await decoder -> (
        match Cachet.next t.cache slice with
        | Some ({ payload; length; _ } as slice) ->
            let decoder = Zl.Inf.src decoder (payload :> Bstr.t) 0 length in
            (go [@tailcall]) ~real_length ~flushed slice decoder
        | None ->
            let decoder = Zl.Inf.src decoder Bstr.empty 0 0 in
            (go [@tailcall]) ~real_length ~flushed slice decoder)
  in
  Log.debug (fun m -> m "load %08x" cursor);
  match Cachet.load t.cache cursor with
  | Some ({ offset; payload; length } as slice) ->
      let off = cursor - offset in
      let len = length - off in
      let decoder = Zl.Inf.decoder `Manual ~o ~allocate:t.allocate in
      let decoder = Zl.Inf.src decoder (payload :> Bstr.t) off len in
      go ~real_length:0 ~flushed:false slice decoder
  | None -> assert false

let of_delta t kind blob ~depth ~cursor =
  let decoder = Zh.M.decoder ~o:t.tmp ~allocate:t.allocate `Manual in
  let rec go slice blob decoder =
    match Zh.M.decode decoder with
    | `End decoder ->
        let len = Zh.M.dst_len decoder in
        { Value.kind; blob; len; depth }
    | `Malformed err -> failwith err
    | `Header (src_len, dst_len, decoder) ->
        let source = Blob.source blob in
        let payload = Blob.payload blob in
        Log.debug (fun m ->
            m "specify the source to apply the patch on %08x" cursor);
        Log.debug (fun m ->
            m "src_len: %d, required: %d" (Bigarray.Array1.dim source) src_len);
        let decoder = Zh.M.source decoder source in
        Log.debug (fun m -> m "dst_len: %d" dst_len);
        let decoder = Zh.M.dst decoder payload 0 dst_len in
        (go [@tailcall]) slice blob decoder
    | `Await decoder -> begin
        match Cachet.next t.cache slice with
        | None ->
            let decoder = Zh.M.src decoder Bstr.empty 0 0 in
            (go [@tailcall]) slice blob decoder
        | Some ({ payload; length; _ } as slice) ->
            let decoder = Zh.M.src decoder (payload :> Bstr.t) 0 length in
            (go [@tailcall]) slice blob decoder
      end
  in
  Log.debug (fun m -> m "load %08x" cursor);
  match Cachet.load t.cache cursor with
  | Some ({ offset; payload; length } as slice) ->
      let off = cursor - offset in
      let len = length - off in
      let decoder = Zh.M.src decoder (payload :> Bstr.t) off len in
      go slice blob decoder
  | None -> assert false

let rec of_ofs_delta t blob ~anchor ~cursor =
  let rel_offset, cursor' = header_of_ofs_delta t ~cursor in
  let v = of_offset t (Blob.flip blob) ~cursor:(anchor - rel_offset) in
  of_delta t v.Value.kind blob ~depth:(succ v.depth) ~cursor:cursor'

and of_ref_delta t blob ~cursor =
  let uid = header_of_ref_delta t ~cursor in
  let v = of_uid t (Blob.flip blob) ~uid in
  of_delta t v.Value.kind blob ~depth:(succ v.depth)
    ~cursor:(cursor + t.ref_length)

and of_uid t blob ~uid =
  match t.where uid with
  | Local cursor -> of_offset t blob ~cursor
  | Extern (kind, src) ->
      let len = Bstr.length src in
      let dst = Blob.payload blob in
      Bstr.blit src ~src_off:0 dst ~dst_off:0 ~len;
      { Value.kind; blob; len; depth= 1 }

and of_offset t blob ~cursor =
  let (kind, _size), cursor' = header_of_entry t ~cursor in
  Log.debug (fun m -> m "decode a new entry (%d)" kind);
  match kind with
  | 0b000 | 0b101 -> raise Bad_type
  | 0b001 -> uncompress t `A blob ~cursor:cursor'
  | 0b010 -> uncompress t `B blob ~cursor:cursor'
  | 0b011 -> uncompress t `C blob ~cursor:cursor'
  | 0b100 -> uncompress t `D blob ~cursor:cursor'
  | 0b110 -> of_ofs_delta t blob ~anchor:cursor ~cursor:cursor'
  | 0b111 -> of_ref_delta t blob ~cursor:cursor'
  | _ -> assert false

module Path = struct
  type t = { path: int array; depth: int; kind: Kind.t; size: Size.t }

  let to_list { path; depth; _ } = Array.sub path 0 depth |> Array.to_list

  let base { depth; path; _ } =
    if depth <= 0 then invalid_arg "The given path is empty";
    path.(depth - 1)

  let kind { kind; _ } = kind
  let size { size; _ } = size
end

let kind_of_int = function
  | 0b001 -> `A
  | 0b010 -> `B
  | 0b011 -> `C
  | 0b100 -> `D
  | _ -> assert false

type result = { kind: Kind.t; size: Size.t; depth: int }

let rec fill_path_from_ofs_delta t (visited : Visited.t) ~anchor ~cursor size =
  let rel_offset, cursor' = header_of_ofs_delta t ~cursor in
  let size = size_of_delta t ~cursor:cursor' size in
  Log.debug (fun m ->
      m "jump to %08x (rel:%08x)" (anchor - rel_offset) rel_offset);
  (fill_path_from_offset [@tailcall]) t visited ~cursor:(anchor - rel_offset)
    size

and fill_path_from_ref_delta t visited ~cursor size =
  let uid = header_of_ref_delta t ~cursor in
  let size = size_of_delta t ~cursor:(cursor + t.ref_length) size in
  (fill_path_from_uid [@tailcall]) t visited ~uid size

and fill_path_from_uid t visited ~uid size =
  match t.where uid with
  | Local cursor -> (fill_path_from_offset [@tailcall]) t visited ~cursor size
  | Extern (kind, bstr) ->
      let visited = { visited with depth= succ visited.depth } in
      { kind; size= Size.max (Bstr.length bstr) size; depth= visited.depth }

and fill_path_from_offset t visited ~cursor size =
  if Visited.already_visited visited ~cursor then raise Cycle;
  visited.path.(visited.depth) <- cursor;
  let visited = { visited with depth= succ visited.depth } in
  if visited.depth >= Array.length visited.path then raise Too_deep;
  let (kind, size'), cursor' = header_of_entry t ~cursor in
  Log.debug (fun m -> m "path[%d]: %08x" (visited.depth - 1) cursor);
  Log.debug (fun m -> m "path[%d].size: %d" (visited.depth - 1) size');
  match kind with
  | 0b000 | 0b101 -> raise Bad_type
  | (0b001 | 0b010 | 0b011 | 0b100) as v ->
      let kind = kind_of_int v in
      Log.debug (fun m ->
          m "path[%d].kind: %a" (visited.depth - 1) Kind.pp kind);
      { kind; size= Size.max size size'; depth= visited.depth }
  | 0b110 ->
      (fill_path_from_ofs_delta [@tailcall]) t visited ~anchor:cursor
        ~cursor:cursor' (Size.max size size')
  | 0b111 ->
      (fill_path_from_ref_delta [@tailcall]) t visited ~cursor:cursor'
        (Size.max size size')
  | _ -> assert false

let path_of_offset ?(max_depth = _max_depth) t ~cursor =
  let visited = { Visited.depth= 0; path= Array.make max_depth 0 } in
  let { kind; size; depth } =
    fill_path_from_offset t visited ~cursor Size.zero
  in
  { Path.depth; path= visited.path; kind; size }

let path_of_uid t uid =
  match t.where uid with
  | Local cursor -> path_of_offset t ~cursor
  | Extern _ -> Fmt.failwith "%a is a not a part of the PACK file" Uid.pp uid

let of_offset_with_source t kind blob ~depth ~cursor =
  let (kind', _size), cursor' = header_of_entry t ~cursor in
  match kind' with
  | 0b000 | 0b101 -> raise Bad_type
  | 0b001 ->
      assert (kind = `A);
      uncompress t `A blob ~cursor:cursor'
  | 0b010 ->
      assert (kind = `B);
      uncompress t `B blob ~cursor:cursor'
  | 0b011 ->
      assert (kind = `C);
      uncompress t `C blob ~cursor:cursor'
  | 0b100 ->
      assert (kind = `D);
      uncompress t `D blob ~cursor:cursor'
  | 0b110 ->
      let _rel_offset, cursor'' = header_of_ofs_delta t ~cursor:cursor' in
      of_delta t kind blob ~depth ~cursor:cursor''
  | 0b111 -> of_delta t kind blob ~depth ~cursor:(cursor' + t.ref_length)
  | _ -> assert false

let base_of_offset t blob ~cursor =
  Log.debug (fun m -> m "load base at %08x" cursor);
  let (kind, _size), cursor' = header_of_entry t ~cursor in
  let kind = kind_of_int kind in
  Log.debug (fun m -> m "uncompress at %08x" cursor');
  uncompress t kind blob ~cursor:cursor'

let of_offset_with_path t ~path blob ~cursor =
  assert (cursor = path.Path.path.(0));
  let max_depth = path.Path.depth in
  let base = base_of_offset t blob ~cursor:(Path.base path) in
  let rec go rdepth blob =
    let cursor = path.path.(rdepth - 1) in
    let depth = max_depth - rdepth in
    let v = of_offset_with_source t base.kind blob ~depth ~cursor in
    if rdepth == 1 then v else (go [@tailcall]) (pred rdepth) (Blob.flip blob)
  in
  if path.depth > 1 then go (path.depth - 1) (Blob.flip blob) else base

let of_offset_with_source t { Value.kind; blob; depth; _ } ~cursor =
  of_offset_with_source t kind blob ~depth ~cursor

type identify = Identify : 'ctx First_pass.identify -> identify

let uid_of_offset ~identify:(Identify gen) t blob ~cursor =
  let (kind', _), cursor' = header_of_entry t ~cursor in
  let kind =
    match kind' with
    | 0b001 -> `A
    | 0b010 -> `B
    | 0b011 -> `C
    | 0b100 -> `D
    | _ -> raise Bad_type
  in
  let value = uncompress t kind blob ~cursor:cursor' in
  let bstr = Blob.payload blob in
  let ctx = gen.First_pass.init kind value.len in
  let ctx = gen.First_pass.feed (Bigarray.Array1.sub bstr 0 value.len) ctx in
  let uid = gen.First_pass.serialize ctx in
  (kind, uid)

let identify (Identify gen) ~kind ~len bstr =
  let ctx = gen.First_pass.init kind len in
  let ctx = gen.First_pass.feed (Bigarray.Array1.sub bstr 0 len) ctx in
  gen.First_pass.serialize ctx

let uid_of_offset_with_source ~identify:gen t ~kind blob ~depth ~cursor =
  let (kind', _), cursor' = header_of_entry t ~cursor in
  match kind' with
  | 0b000 | 0b101 -> raise Bad_type
  | 0b001 ->
      assert (kind = `A);
      assert (depth = 1);
      let v = uncompress t `A blob ~cursor:cursor' in
      identify gen ~kind ~len:v.len (Blob.payload blob)
  | 0b010 ->
      assert (kind = `B);
      assert (depth = 1);
      let v = uncompress t `B blob ~cursor:cursor' in
      identify gen ~kind ~len:v.len (Blob.payload blob)
  | 0b011 ->
      assert (kind = `C);
      assert (depth = 1);
      let v = uncompress t `C blob ~cursor:cursor' in
      identify gen ~kind ~len:v.len (Blob.payload blob)
  | 0b100 ->
      assert (kind = `D);
      assert (depth = 1);
      let v = uncompress t `D blob ~cursor:cursor' in
      identify gen ~kind ~len:v.len (Blob.payload blob)
  | 0b110 ->
      let _, cursor'' = header_of_ofs_delta t ~cursor:cursor' in
      let { Value.blob; len; _ } =
        of_delta t kind blob ~depth ~cursor:cursor''
      in
      identify gen ~kind ~len (Blob.payload blob)
  | 0b111 ->
      let { Value.blob; len; _ } =
        of_delta t kind blob ~depth ~cursor:(cursor' + t.ref_length)
      in
      identify gen ~kind ~len (Blob.payload blob)
  | _ -> assert false

type children = cursor:int -> uid:Uid.t -> int list
type where = cursor:int -> int

type oracle = {
    identify: identify
  ; children: children
  ; where: where
  ; cursor: pos:int -> int
  ; size: cursor:int -> int
  ; checksum: cursor:int -> Optint.t
  ; is_base: pos:int -> int option
  ; number_of_objects: int
  ; hash: string
}

type status =
  | Unresolved_base of { cursor: int }
  | Unresolved_node of { cursor: int }
  | Resolved_base of { cursor: int; uid: Uid.t; crc: Optint.t; kind: Kind.t }
  | Resolved_node of {
        cursor: int
      ; uid: Uid.t
      ; crc: Optint.t
      ; kind: Kind.t
      ; depth: int
      ; parent: Uid.t
    }
