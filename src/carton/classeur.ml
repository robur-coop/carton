let invalid_argf fmt = Format.kasprintf invalid_arg fmt
let src = Logs.Src.create "carton.idx"

module Log = (val Logs.src_log src : Logs.LOG)

type uid = string
type 'a fn = uid:uid -> crc:Optint.t -> offset:int -> 'a

let error_msgf fmt = Format.kasprintf (fun msg -> Error (`Msg msg)) fmt

type 'fd t = {
    cache: 'fd Cachet.t
  ; max: int
  ; ref_length: int
  ; crcs_offset: int
  ; values_offset: int
  ; pack: string
  ; idx: string
}

let make ?pagesize ?cachesize ~map fd ~length ~hash_length ~ref_length =
  let cache = Cachet.make ?pagesize ?cachesize ~map fd in
  let header = Cachet.get_int32_be cache 0 in
  let version = Cachet.get_int32_be cache 4 in
  let max = Int32.to_int (Cachet.get_int32_be cache (8 + (255 * 4))) in
  if header <> 0xff744f63l then invalid_arg "Invalid IDX file";
  if version <> 0x2l then invalid_arg "Invalid version of IDX file";
  let crcs_offset = 8 + (256 * 4) + (max * ref_length) in
  let values_offset = 8 + (256 * 4) + (max * ref_length) + (max * 4) in
  let pack =
    Cachet.get_string cache ~len:hash_length (length - (hash_length * 2))
  in
  let idx = Cachet.get_string cache ~len:hash_length (length - hash_length) in
  { cache; max; ref_length; crcs_offset; values_offset; pack; idx }

let of_cachet ~length ~hash_length ~ref_length cache =
  let header = Cachet.get_int32_be cache 0 in
  let version = Cachet.get_int32_be cache 4 in
  let max = Int32.to_int (Cachet.get_int32_be cache (8 + (255 * 4))) in
  if header <> 0xff744f63l then invalid_arg "Invalid IDX file";
  if version <> 0x2l then invalid_arg "Invalid version of IDX file";
  let crcs_offset = 8 + (256 * 4) + (max * ref_length) in
  let values_offset = 8 + (256 * 4) + (max * ref_length) + (max * 4) in
  let pack =
    Cachet.get_string cache ~len:hash_length (length - (hash_length * 2))
  in
  let idx = Cachet.get_string cache ~len:hash_length (length - hash_length) in
  { cache; max; ref_length; crcs_offset; values_offset; pack; idx }

let copy t = { t with cache= Cachet.copy t.cache }

let uid_of_string t uid =
  if String.length uid = t.ref_length then Ok uid
  else
    error_msgf "Invalid unique identifier according to the given IDX file: %s"
      (Ohex.encode uid)

let uid_of_string_exn t uid =
  match uid_of_string t uid with
  | Ok uid -> uid
  | Error (`Msg msg) -> failwith msg

let unsafe_uid_of_string uid = uid
let fanout_offset = 8
let hashes_offset = 8 + (256 * 4)
let _max_int31 = 2147483647l
let _max_int63 = 9223372036854775807L

let search t uid =
  let n = Char.code uid.[0] in
  let a =
    match n with
    | 0 -> 0l
    | n -> Cachet.get_int32_be t.cache (fanout_offset + (4 * (n - 1)))
  in
  let b = Cachet.get_int32_be t.cache (fanout_offset + (4 * n)) in
  let abs_off = hashes_offset + (Int32.to_int a * t.ref_length) in
  let len = Int32.(to_int (sub b a)) * t.ref_length in
  Log.debug (fun m -> m "search %s" (Ohex.encode uid));
  let rec go sub_off sub_len =
    Log.debug (fun m -> m "sub_off:%08x, sub_len:%08x" sub_off sub_len);
    let len = sub_len / (2 * t.ref_length) * t.ref_length in
    (* XXX(dinosaure): prevent a wrong comparison with something outside the
       hashes table. *)
    if sub_off + len = hashes_offset + (t.ref_length * t.max) then
      raise_notrace Not_found;
    let uid' = Cachet.get_string t.cache ~len:t.ref_length (sub_off + len) in
    Log.debug (fun m ->
        m "compare with (len: %d) (off: %d) %s" len sub_off (Ohex.encode uid'));
    let cmp = String.compare uid uid' in
    Log.debug (fun m -> m "compare: %d" cmp);
    if cmp == 0 then sub_off + len
    else if sub_len <= t.ref_length then raise_notrace Not_found
    else if cmp < 0 then (go [@tailcall]) sub_off len
    else (go [@tailcall]) (sub_off + len) (sub_len - len)
  in
  let off = go abs_off len in
  Int32.to_int a + ((off - abs_off) / t.ref_length)

let[@inline never] too_huge () =
  failwith "The PACK file associated to the IDX file is too huge"

let find_offset t uid =
  let n = search t uid in
  let off = Cachet.get_int32_be t.cache (t.values_offset + (n * 4)) in
  if Sys.word_size == 32 && Int32.logand off 0x80000000l <> 0l then too_huge ();
  if Int32.logand off 0x80000000l <> 0l then begin
    let off = Int32.to_int off land 0x7fffffff in
    let off =
      Cachet.get_int64_be t.cache (t.values_offset + (t.max * 4) + (off * 8))
    in
    if off > _max_int63 then too_huge ();
    Int64.to_int off
  end
  else if Sys.word_size == 32 && off > _max_int31 then too_huge ()
  else Int32.to_int off

let find_crc t uid =
  let n = search t uid in
  let crc = Cachet.get_int32_be t.cache (t.crcs_offset + (n * 4)) in
  Optint.of_unsigned_int32 crc

let exists t uid =
  try
    let _ = search t uid in
    true
  with Not_found -> false

let get_uid t pos =
  let len = t.ref_length in
  Cachet.get_string t.cache (hashes_offset + (pos * t.ref_length)) ~len

let get_offset t pos =
  let off = Cachet.get_int32_be t.cache (t.values_offset + (pos * 4)) in
  if Sys.word_size == 32 && Int32.logand off 0x80000000l <> 0l then too_huge ();
  if Int32.logand off 0x80000000l <> 0l then begin
    let off = Int32.to_int off land 0x7fffffff in
    let off =
      Cachet.get_int64_be t.cache (t.values_offset + (t.max * 4) + (off * 8))
    in
    if off > _max_int63 then too_huge ();
    Int64.to_int off
  end
  else if Sys.word_size == 32 && off > _max_int31 then too_huge ()
  else Int32.to_int off

let get_crc t pos =
  let crc = Cachet.get_int32_be t.cache (t.crcs_offset + (pos * 4)) in
  Optint.of_unsigned_int32 crc

let get t pos =
  if pos >= t.max then invalid_arg "Classeur.get";
  let uid = get_uid t pos
  and crc = get_crc t pos
  and offset = get_offset t pos in
  (uid, crc, offset)

let max { max; _ } = max

let iter ~fn t =
  for i = 0 to t.max - 1 do
    let uid = get_uid t i and crc = get_crc t i and offset = get_offset t i in
    fn ~uid ~crc ~offset
  done

let map ~fn t =
  let rec go acc = function
    | 0 -> List.rev acc
    | n ->
        let i = n - 1 in
        let uid = get_uid t i
        and crc = get_crc t i
        and offset = get_offset t i in
        go (fn ~uid ~crc ~offset :: acc) (n - 1)
  in
  go [] t.max

let pack t = t.pack
let idx t = t.idx

module type UID = sig
  type t
  type ctx

  val empty : ctx
  val feed : ctx -> ?off:int -> ?len:int -> Bstr.t -> ctx
  val get : ctx -> t
  val compare : t -> t -> int
  val length : int
  val to_raw_string : t -> string
  val pp : Format.formatter -> t -> unit
end

[@@@warning "-69"]

module Encoder = struct
  type dst = [ `Channel of out_channel | `Buffer of Buffer.t | `Manual ]

  type 'ctx hash = 'ctx Carton.First_pass.hash = {
      feed_bytes: bytes -> off:int -> len:int -> 'ctx -> 'ctx
    ; feed_bigstring: De.bigstring -> 'ctx -> 'ctx
    ; serialize: 'ctx -> string
    ; length: int
  }

  type digest = Carton.First_pass.digest = Digest : 'ctx hash * 'ctx -> digest
  type entry = { crc: Optint.t; offset: int64; uid: uid }

  type encoder = {
      dst: dst
    ; mutable out: bytes
    ; mutable out_off: int
    ; mutable out_pos: int
    ; mutable out_max: int
    ; tmp: bytes
    ; mutable tmp_pos: int
    ; mutable tmp_max: int
    ; queue: int64 Queue.t
    ; mutable counter_of_objects: int
    ; fanout: int array
    ; index: entry array
    ; pack: string
    ; ref_length: int
    ; mutable digest: digest
    ; mutable k: encoder -> [ `Await ] -> [ `Partial | `Ok ]
  }

  let digest buf ?(off = 0) ?len (Digest (({ feed_bytes; _ } as hash), ctx)) =
    let default = Bytes.length buf - off in
    let len = Option.value ~default len in
    Digest (hash, feed_bytes ~off ~len buf ctx)

  let dst encoder dst off len =
    if off < 0 || len < 0 || off + len > Bytes.length dst then
      invalid_argf "Out of bounds (off: %d, len: %d)" off len;
    encoder.out <- dst;
    encoder.out_off <- off;
    encoder.out_pos <- off;
    encoder.out_max <- off + len - 1

  let partial k encoder = function `Await -> k encoder

  let digest_encoder encoder ~len =
    let digest = digest encoder.out ~off:0 ~len encoder.digest in
    encoder.digest <- digest

  let flush_with_digest k encoder =
    match encoder.dst with
    | `Manual ->
        digest_encoder encoder ~len:encoder.out_pos;
        encoder.k <- partial k;
        `Partial
    | `Channel oc ->
        let len = encoder.out_pos in
        digest_encoder encoder ~len;
        let str = Bytes.sub_string encoder.out 0 len in
        output_string oc str;
        encoder.out_pos <- 0;
        k encoder
    | `Buffer buf ->
        let len = encoder.out_pos in
        digest_encoder encoder ~len;
        let str = Bytes.sub_string encoder.out 0 len in
        Buffer.add_string buf str;
        encoder.out_pos <- 0;
        k encoder

  let flush k encoder =
    match encoder.dst with
    | `Manual ->
        encoder.k <- partial k;
        `Partial
    | `Channel oc ->
        let len = encoder.out_pos in
        let str = Bytes.sub_string encoder.out 0 len in
        output_string oc str;
        encoder.out_pos <- 0;
        k encoder
    | `Buffer buf ->
        let len = encoder.out_pos in
        let str = Bytes.sub_string encoder.out 0 len in
        Buffer.add_string buf str;
        encoder.out_pos <- 0;
        k encoder

  let out_rem encoder = encoder.out_max - encoder.out_pos + 1

  let tmp_range encoder m =
    encoder.tmp_pos <- 0;
    encoder.tmp_max <- m

  let rec tmp_flush ?(with_digest = true) k encoder =
    let blit encoder len =
      let src_off = encoder.tmp_pos and dst_off = encoder.out_pos in
      Bytes.blit encoder.tmp src_off encoder.out dst_off len;
      encoder.out_pos <- encoder.out_pos + len;
      encoder.tmp_pos <- encoder.tmp_pos + len
    in
    let rem = out_rem encoder in
    let len = encoder.tmp_max - encoder.tmp_pos + 1 in
    let flush = if with_digest then flush_with_digest else flush in
    if rem < len then begin
      blit encoder rem;
      flush (tmp_flush k) encoder
    end
    else (blit encoder len; k encoder)

  let ok encoder =
    encoder.k <- (fun _ `Await -> `Ok);
    `Ok

  let serialize (Digest ({ serialize; _ }, ctx)) = serialize ctx
  let length_of_hash (Digest ({ length; _ }, _)) = length

  let encode_trail encoder `Await =
    let len = length_of_hash encoder.digest in
    let k2 encoder = flush ok encoder in
    let k1 encoder =
      let rem = out_rem encoder in
      let dst, dst_off, k =
        if rem < len then begin
          tmp_range encoder (len - 1);
          (encoder.tmp, 0, tmp_flush ~with_digest:false k2)
        end
        else
          let off = encoder.out_pos in
          encoder.out_pos <- encoder.out_pos + len;
          (encoder.out, off, k2)
      in
      let uid = serialize encoder.digest in
      Bytes.blit_string uid 0 dst dst_off len;
      k encoder
    in
    let k0 encoder = flush_with_digest k1 encoder in
    let rem = out_rem encoder in
    let dst, dst_off, k =
      if rem < len then begin
        tmp_range encoder (len - 1);
        (encoder.tmp, 0, tmp_flush k0)
      end
      else
        let off = encoder.out_pos in
        encoder.out_pos <- encoder.out_pos + len;
        (encoder.out, off, k0)
    in
    let src = (encoder.pack :> string) in
    Bytes.blit_string src 0 dst dst_off len;
    k encoder

  let rec encode_big_offset encoder `Await =
    let offset = Queue.pop encoder.queue in
    let k encoder =
      if Queue.is_empty encoder.queue then encode_trail encoder `Await
      else encode_big_offset encoder `Await
    in
    let rem = out_rem encoder in
    let dst, dst_off, k =
      if rem < 8 then begin
        tmp_range encoder 7;
        (encoder.tmp, 0, tmp_flush k)
      end
      else
        let off = encoder.out_pos in
        encoder.out_pos <- encoder.out_pos + 8;
        (encoder.out, off, k)
    in
    Bytes.set_int64_be dst dst_off offset;
    k encoder

  let rec encode_offset encoder `Await =
    let k encoder =
      if encoder.counter_of_objects + 1 == Array.length encoder.index then begin
        encoder.counter_of_objects <- 0;
        if Queue.is_empty encoder.queue then encode_trail encoder `Await
        else encode_big_offset encoder `Await
      end
      else begin
        encoder.counter_of_objects <- succ encoder.counter_of_objects;
        encode_offset encoder `Await
      end
    in
    let rem = out_rem encoder in
    let dst, dst_off, k =
      if rem < 4 then begin
        tmp_range encoder 3;
        (encoder.tmp, 0, tmp_flush k)
      end
      else
        let off = encoder.out_pos in
        encoder.out_pos <- encoder.out_pos + 4;
        (encoder.out, off, k)
    in
    let { offset; _ } = encoder.index.(encoder.counter_of_objects) in
    if Int64.shift_right_logical offset 31 <> 0L then begin
      let n = Queue.length encoder.queue in
      Queue.push offset encoder.queue;
      let value = Int32.(logor 0x80000000l (of_int n)) in
      Bytes.set_int32_be dst dst_off value;
      k encoder
    end
    else
      let value = Int64.to_int32 offset in
      Bytes.set_int32_be dst dst_off value;
      k encoder

  let rec encode_crc encoder `Await =
    let k encoder =
      if encoder.counter_of_objects + 1 == Array.length encoder.index then begin
        encoder.counter_of_objects <- 0;
        encode_offset encoder `Await
      end
      else begin
        encoder.counter_of_objects <- succ encoder.counter_of_objects;
        encode_crc encoder `Await
      end
    in
    let rem = out_rem encoder in
    let dst, dst_off, k =
      if rem < 4 then begin
        tmp_range encoder 3;
        (encoder.tmp, 0, tmp_flush k)
      end
      else
        let off = encoder.out_pos in
        encoder.out_pos <- encoder.out_pos + 4;
        (encoder.out, off, k)
    in
    let { crc; _ } = encoder.index.(encoder.counter_of_objects) in
    let crc = Optint.to_unsigned_int32 crc in
    Bytes.set_int32_be dst dst_off crc;
    k encoder

  let rec encode_hash encoder `Await =
    let k encoder =
      if encoder.counter_of_objects + 1 == Array.length encoder.index then begin
        encoder.counter_of_objects <- 0;
        encode_crc encoder `Await
      end
      else begin
        encoder.counter_of_objects <- succ encoder.counter_of_objects;
        encode_hash encoder `Await
      end
    in
    let rem = out_rem encoder in
    let dst, dst_off, k =
      if rem < encoder.ref_length then begin
        tmp_range encoder (encoder.ref_length - 1);
        (encoder.tmp, 0, tmp_flush k)
      end
      else
        let off = encoder.out_pos in
        encoder.out_pos <- encoder.out_pos + encoder.ref_length;
        (encoder.out, off, k)
    in
    let { uid; _ } = encoder.index.(encoder.counter_of_objects) in
    let len = encoder.ref_length in
    Bytes.blit_string uid 0 dst dst_off len;
    k encoder

  let rec encode_fanout encoder `Await =
    let k encoder =
      if encoder.counter_of_objects + 1 == 256 then begin
        encoder.counter_of_objects <- 0;
        if Array.length encoder.index > 0 then encode_hash encoder `Await
        else encode_trail encoder `Await
      end
      else begin
        encoder.counter_of_objects <- succ encoder.counter_of_objects;
        encode_fanout encoder `Await
      end
    in
    let rem = out_rem encoder in
    let dst, dst_off, k =
      if rem < 4 then begin
        tmp_range encoder 3;
        (encoder.tmp, 0, tmp_flush k)
      end
      else
        let off = encoder.out_pos in
        encoder.out_pos <- encoder.out_pos + 4;
        (encoder.out, off, k)
    in
    let value =
      let acc = ref 0 in
      for i = 0 to encoder.counter_of_objects do
        acc := !acc + encoder.fanout.(i)
      done;
      !acc
    in
    let value = Int32.of_int value in
    Bytes.set_int32_be dst dst_off value;
    k encoder

  let encode_header encoder `Await =
    let k encoder =
      encoder.counter_of_objects <- 0;
      encode_fanout encoder `Await
    in
    let rem = out_rem encoder in
    let dst, dst_off, k =
      if rem < 8 then begin
        tmp_range encoder 8;
        (encoder.tmp, 0, tmp_flush k)
      end
      else
        let off = encoder.out_pos in
        encoder.out_pos <- encoder.out_pos + 8;
        (encoder.out, off, k)
    in
    Bytes.set_int32_be dst dst_off 0xff744f63l;
    Bytes.set_int32_be dst (dst_off + 4) 0x2l;
    k encoder

  let encoder dst ~digest ~pack ~ref_length index =
    let compare { uid= a; _ } { uid= b; _ } = String.compare a b in
    Array.sort compare index;
    let fanout = Array.make 256 0 in
    Array.iter
      (fun { uid; _ } ->
        let n = Char.code (uid :> string).[0] in
        fanout.(n) <- fanout.(n) + 1)
      index;
    let out, out_pos, out_max =
      match dst with
      | `Manual -> (Bytes.empty, 1, 0)
      | `Buffer _ | `Channel _ ->
          let buf = Bytes.make 0x7ff '\000' in
          (buf, 0, Bytes.length buf - 1)
    in
    let tmp = Bytes.make (Int.max ref_length (length_of_hash digest)) '\000' in
    {
      dst
    ; out
    ; out_off= 0
    ; out_pos
    ; out_max
    ; tmp
    ; queue= Queue.create ()
    ; tmp_pos= 1
    ; tmp_max= 0
    ; counter_of_objects= 0
    ; fanout
    ; index
    ; pack
    ; digest
    ; ref_length
    ; k= encode_header
    }

  let dst_rem = out_rem
  let encode e = e.k e
end
