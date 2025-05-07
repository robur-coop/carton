let invalid_argf fmt = Format.kasprintf invalid_arg fmt
let invalid_encode () = invalid_argf "expected `Await encode"

let invalid_bounds off len bstr =
  invalid_argf "Out of bounds (off: %d, len: %d, real-len: %d)" off len
    (Bstr.length bstr)

external ( < ) : 'a -> 'a -> bool = "%lessthan"
external ( <= ) : 'a -> 'a -> bool = "%lessequal"
external ( > ) : 'a -> 'a -> bool = "%greaterthan"

let ( > ) (x : int) y = x > y [@@inline]
let ( < ) (x : int) y = x < y [@@inline]
let ( <= ) (x : int) y = x <= y [@@inline]

module M = struct
  type src = [ `Manual | `String of string ]
  type decode = [ `Await | `Header of int * int | `End | `Malformed of string ]

  type decoder = {
      mutable source: Bstr.t
    ; src: src
    ; mutable dst: Bstr.t
    ; mutable i: Bstr.t
    ; mutable i_pos: int
    ; mutable i_len: int
    ; mutable t_len: int
    ; mutable t_need: int
    ; t_tmp: Bstr.t
    ; mutable o_pos: int
    ; mutable src_len: int
    ; mutable dst_len: int
    ; mutable s: state
    ; mutable k: decoder -> ret
  }

  and ret = Await | Stop | End | Malformed of string
  and state = Header | Postprocess | Cmd | Cp of int | It of int

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
  [@@inline]

  let eoi d =
    d.i <- Bstr.empty;
    d.i_pos <- 0;
    d.i_len <- min_int

  let i_rem d = d.i_len - d.i_pos + 1 [@@inline]
  let src_rem = i_rem
  let dst_rem d = Bstr.length d.dst - d.o_pos
  let src_len { src_len; _ } = src_len
  let dst_len { dst_len; _ } = dst_len
  let malformedf fmt = Format.kasprintf (fun s -> Malformed s) fmt

  let t_need d n =
    d.t_len <- 0;
    d.t_need <- n

  let src d s j l =
    if j < 0 || l < 0 || j + l > Bstr.length s then invalid_bounds j l s;
    if l == 0 then eoi d
    else (
      d.i <- s;
      d.i_pos <- j;
      d.i_len <- j + l - 1)

  let dst d bstr off len =
    Logs.debug (fun m -> m "off:%d, len:%d" off len);
    match d.s with
    | Postprocess ->
        if off < 0 || len < 0 || off + len > Bstr.length bstr then
          invalid_bounds off len bstr;
        if len < d.dst_len then invalid_argf "Invalid destination";
        d.dst <- bstr;
        d.o_pos <- off;
        if Bstr.length d.source >= d.src_len then d.s <- Cmd
    | _ -> invalid_argf "Invalid call of dst"

  let pp_state ppf = function
    | Header -> Format.pp_print_string ppf "Header"
    | Postprocess -> Format.pp_print_string ppf "Postprocess"
    | Cmd -> Format.pp_print_string ppf "Cmd"
    | Cp _ -> Format.pp_print_string ppf "Cd"
    | It _ -> Format.pp_print_string ppf "It"

  let source d src =
    match d.s with
    | Postprocess ->
        if Bstr.length src < d.src_len then invalid_argf "Invalid source"
        else d.source <- src
    | _ -> invalid_argf "Invalid call of source (state: %a)" pp_state d.s

  (* get new input in [d.i] and [k]ontinue. *)
  let refill k d =
    match d.src with
    | `String _ -> eoi d; k d
    | `Manual ->
        d.k <- k;
        Await

  let rec t_fill k d =
    let blit d len =
      Bstr.blit d.i ~src_off:d.i_pos d.t_tmp ~dst_off:d.t_len ~len;
      d.i_pos <- d.i_pos + len;
      d.t_len <- d.t_len + len
    in
    let rem = i_rem d in
    if rem < 0 then k d
    else
      let need = d.t_need - d.t_len in
      if rem < need then (
        blit d rem;
        refill (t_fill k) d)
      else (blit d need; k d)

  let required =
    let a = [| 0; 1; 1; 2; 1; 2; 2; 3; 1; 2; 2; 3; 2; 3; 3; 4 |] in
    fun x -> a.(x land 0xf) + a.(x lsr 4)

  let enough d =
    match d.s with
    | Cp cmd -> i_rem d >= required (cmd land 0x7f)
    | It len -> i_rem d >= len
    | _ -> assert false

  (* XXX(dinosaure): [enough] is called only after a [d.s <- (It _ | Cp _)]. *)

  let need d =
    match d.s with
    | Cp cmd -> required (cmd land 0x7f)
    | It len -> len
    | _ -> assert false

  (* XXX(dinosaure): [flambda] is able to optimize [let rec a .. and b .. and c ..]
     instead [match .. with A -> .. | B -> .. | C -> ..]. *)

  let rec cp d =
    let[@warning "-8"] (Cp command) = d.s in
    let p = ref (if d.t_len > 0 then 0 else d.i_pos) in
    let i = if d.t_len > 0 then d.t_tmp else d.i in
    let cp_off = ref 0 in
    let cp_len = ref 0 in
    if command land 0x01 != 0 then (
      let v = Bstr.get_uint8 i !p in
      cp_off := v;
      incr p);
    if command land 0x02 != 0 then (
      let v = Bstr.get_uint8 i !p in
      cp_off := !cp_off lor (v lsl 8);
      incr p);
    if command land 0x04 != 0 then (
      let v = Bstr.get_uint8 i !p in
      cp_off := !cp_off lor (v lsl 16);
      incr p);
    if command land 0x08 != 0 then (
      let v = Bstr.get_uint8 i !p in
      cp_off := !cp_off lor (v lsl 24);
      incr p);
    if command land 0x10 != 0 then (
      let v = Bstr.get_uint8 i !p in
      cp_len := v;
      incr p);
    if command land 0x20 != 0 then (
      let v = Bstr.get_uint8 i !p in
      cp_len := !cp_len lor (v lsl 8);
      incr p);
    if command land 0x40 != 0 then (
      let v = Bstr.get_uint8 i !p in
      cp_len := !cp_len lor (v lsl 16);
      incr p);
    if !cp_len == 0 then cp_len := 0x10000;
    Bstr.blit d.source ~src_off:!cp_off d.dst ~dst_off:d.o_pos ~len:!cp_len;
    if d.t_len > 0 then d.t_len <- 0 else d.i_pos <- !p;
    d.o_pos <- d.o_pos + !cp_len;
    d.s <- Cmd;
    d.k <- decode_k;
    decode_k d

  and it d =
    let[@warning "-8"] (It len) = d.s in
    if d.t_len > 0 then begin
      Bstr.blit d.t_tmp ~src_off:0 d.dst ~dst_off:d.o_pos ~len;
      d.t_len <- 0;
      d.o_pos <- d.o_pos + len;
      d.s <- Cmd;
      d.k <- decode_k;
      decode_k d
    end
    else begin
      Bstr.blit d.i ~src_off:d.i_pos d.dst ~dst_off:d.o_pos ~len;
      d.i_pos <- d.i_pos + len;
      d.o_pos <- d.o_pos + len;
      d.s <- Cmd;
      d.k <- decode_k;
      decode_k d
    end

  and cmd d =
    let c = Bstr.get_uint8 d.i d.i_pos in
    if c == 0 then malformedf "Invalid delta code (%02x)" c
    else (
      d.s <- (if c land 0x80 != 0 then Cp c else It c);
      d.i_pos <- d.i_pos + 1;
      if enough d then if c land 0x80 != 0 then cp d else it d
      else (
        t_need d (need d);
        t_fill (if c land 0x80 != 0 then cp else it) d))

  and decode_k d =
    let rem = i_rem d in
    if rem <= 0 then if rem < 0 then End else refill decode_k d
    else
      match d.s with
      | Header ->
          if rem < 2 then invalid_argf "Not enough space";
          (* TODO: [malformedf]? *)
          let x, src_len = variable_length d.i d.i_pos d.i_len in
          let y, dst_len = variable_length d.i (d.i_pos + x) d.i_len in
          (* XXX(dinosaure): ok, this code can only work if the first given buffer
             is large enough to store header. In the case of [carton], output buffer
             of [zlib]/input buffer of [h] is [io_buffer_size]. *)
          d.i_pos <- d.i_pos + x + y;
          d.src_len <- src_len;
          d.dst_len <- dst_len;
          d.s <- Postprocess;
          Stop
      | Postprocess -> Stop
      | Cmd -> cmd d
      | Cp cmd ->
          if required (cmd land 0x7f) <= rem then cp d
          else (
            t_need d (need d);
            t_fill cp d)
      | It len ->
          if len <= rem then it d
          else (
            t_need d (need d);
            t_fill it d)

  let decode d =
    match d.k d with
    | Await -> `Await
    | Stop -> `Header (d.src_len, d.dst_len)
    | End -> `End
    | Malformed err -> `Malformed err

  let decoder ?(source = Bstr.empty) src =
    let i, i_pos, i_len =
      match src with
      | `Manual -> (Bstr.empty, 1, 0)
      | `String x -> (Bstr.of_string x, 0, String.length x - 1)
    in
    {
      src
    ; source
    ; dst= Bstr.empty
    ; i
    ; i_pos
    ; i_len
    ; t_len= 0
    ; t_need= 0
    ; t_tmp= Bstr.create 128
    ; o_pos= 0
    ; src_len= 0
    ; dst_len= 0
    ; s= Header
    ; k= decode_k
    }
end

module R = struct
  type src = [ `Manual | `String of string ]

  type decode =
    [ `Await
    | `Header of int * int
    | `Copy of int * int
    | `Insert of string
    | `End
    | `Malformed of string ]

  type decoder = {
      src: src
    ; mutable i: bytes
    ; mutable i_pos: int
    ; mutable i_len: int
    ; mutable t_len: int
    ; mutable t_need: int
    ; t_tmp: bytes
    ; mutable src_len: int
    ; mutable dst_len: int
    ; mutable s: state
    ; mutable k: decoder -> ret
  }

  and ret =
    [ `Await
    | `Header of int * int
    | `Copy of int * int
    | `Insert of string
    | `End
    | `Malformed of string ]

  and state = Header | Cmd | Cp of int | It of int

  let invalid_bounds off len str =
    invalid_argf "H.R: Out of bounds (off: %d, len: %d, real-len: %d)" off len
      (String.length str)

  let eoi decoder =
    decoder.i <- Bytes.empty;
    decoder.i_pos <- 0;
    decoder.i_len <- min_int

  let src_rem decoder = decoder.i_len - decoder.i_pos + 1 [@@inline]
  let src_len { src_len; _ } = src_len
  let dst_len { dst_len; _ } = dst_len
  let malformedf fmt = Format.kasprintf (fun str -> `Malformed str) fmt

  let t_need decoder n =
    decoder.t_len <- 0;
    decoder.t_need <- n

  let src decoder src j l =
    if j < 0 || l < 0 || j + l > String.length src then invalid_bounds j l src;
    if l == 0 then eoi decoder
    else begin
      decoder.i <- Bytes.unsafe_of_string src;
      decoder.i_pos <- j;
      decoder.i_len <- j + l - 1
    end

  let refill k decoder =
    match decoder.src with
    | `String _ -> eoi decoder; k decoder
    | `Manual ->
        decoder.k <- k;
        `Await

  let rec t_fill k decoder =
    let blit decoder len =
      Bytes.blit decoder.i decoder.i_pos decoder.t_tmp decoder.t_len len;
      decoder.i_pos <- decoder.i_pos + len;
      decoder.t_len <- decoder.t_len + len
    in
    let rem = src_rem decoder in
    let need = decoder.t_need - decoder.t_len in
    blit decoder (Int.min rem need);
    if rem >= need then k decoder else refill (t_fill k) decoder

  let enough decoder =
    match decoder.s with
    | Cp cmd -> src_rem decoder >= M.required (cmd land 0x7f)
    | It len -> src_rem decoder >= len
    | _ -> assert false

  let need decoder =
    match decoder.s with
    | Cp cmd -> M.required (cmd land 0x7f)
    | It len -> len
    | _ -> assert false

  let variable_length buf off top =
    let p = ref off in
    let i = ref 0 in
    let len = ref 0 in
    while
      let cmd = Bytes.get_uint8 buf !p in
      incr p;
      len := !len lor ((cmd land 0x7f) lsl !i);
      i := !i + 7;
      cmd land 0x80 != 0 && !p <= top
    do
      ()
    done;
    (!p - off, !len)
  [@@inline]

  let rec cp decoder =
    let[@warning "-8"] (Cp command) = decoder.s in
    let p = ref (if decoder.t_len > 0 then 0 else decoder.i_pos) in
    let i = if decoder.t_len > 0 then decoder.t_tmp else decoder.i in
    let cp_off = ref 0 in
    let cp_len = ref 0 in
    if command land 0x01 != 0 then (
      let v = Bytes.get_uint8 i !p in
      cp_off := v;
      incr p);
    if command land 0x02 != 0 then (
      let v = Bytes.get_uint8 i !p in
      cp_off := !cp_off lor (v lsl 8);
      incr p);
    if command land 0x04 != 0 then (
      let v = Bytes.get_uint8 i !p in
      cp_off := !cp_off lor (v lsl 16);
      incr p);
    if command land 0x08 != 0 then (
      let v = Bytes.get_uint8 i !p in
      cp_off := !cp_off lor (v lsl 24);
      incr p);
    if command land 0x10 != 0 then (
      let v = Bytes.get_uint8 i !p in
      cp_len := v;
      incr p);
    if command land 0x20 != 0 then (
      let v = Bytes.get_uint8 i !p in
      cp_len := !cp_len lor (v lsl 8);
      incr p);
    if command land 0x40 != 0 then (
      let v = Bytes.get_uint8 i !p in
      cp_len := !cp_len lor (v lsl 16);
      incr p);
    if !cp_len == 0 then cp_len := 0x10000;
    if decoder.t_len > 0 then decoder.t_len <- 0 else decoder.i_pos <- !p;
    decoder.s <- Cmd;
    decoder.k <- decode_k;
    `Copy (!cp_off, !cp_len)

  and it decoder =
    let[@warning "-8"] (It len) = decoder.s in
    if decoder.t_len > 0 then begin
      let str = Bytes.sub_string decoder.t_tmp 0 len in
      decoder.s <- Cmd;
      decoder.k <- decode_k;
      `Insert str
    end
    else begin
      let str = Bytes.sub_string decoder.i decoder.i_pos len in
      decoder.s <- Cmd;
      decoder.k <- decode_k;
      `Insert str
    end

  and cmd decoder =
    let c = Bytes.get_uint8 decoder.i decoder.i_pos in
    if c == 0 then malformedf "Invalid delta code (%02x)" c
    else begin
      decoder.s <- (if c land 0x80 != 0 then Cp c else It c);
      decoder.i_pos <- decoder.i_pos + 1;
      if enough decoder then if c land 0x80 != 0 then cp decoder else it decoder
      else begin
        t_need decoder (need decoder);
        t_fill (if c land 0x80 != 0 then cp else it) decoder
      end
    end

  and decode_k decoder =
    let rem = src_rem decoder in
    if rem <= 0 then if rem < 0 then `End else refill decode_k decoder
    else
      match decoder.s with
      | Header ->
          if rem < 2 then invalid_argf "Not enough space";
          (* TODO: [malformedf]? *)
          let x, src_len =
            variable_length decoder.i decoder.i_pos decoder.i_len
          in
          let y, dst_len =
            variable_length decoder.i (decoder.i_pos + x) decoder.i_len
          in
          decoder.i_pos <- decoder.i_pos + x + y;
          decoder.src_len <- src_len;
          decoder.dst_len <- dst_len;
          decoder.s <- Cmd;
          decoder.k <- decode_k;
          `Header (src_len, dst_len)
      | Cmd -> cmd decoder
      | Cp cmd ->
          if M.required (cmd land 0x7f) <= rem then cp decoder
          else begin
            t_need decoder (need decoder);
            t_fill cp decoder
          end
      | It len ->
          if len <= rem then it decoder
          else begin
            t_need decoder (need decoder);
            t_fill it decoder
          end

  let decode decoder = decoder.k decoder

  let decoder src =
    let i, i_pos, i_len =
      match src with
      | `Manual -> (Bytes.empty, 1, 0)
      | `String x -> (Bytes.unsafe_of_string x, 0, String.length x - 1)
    in
    {
      src
    ; i
    ; i_pos
    ; i_len
    ; t_len= 0
    ; t_need= 0
    ; t_tmp= Bytes.create 128
    ; src_len= 0
    ; dst_len= 0
    ; s= Header
    ; k= decode_k
    }

  let of_seq seq =
   fun () ->
    let decoder = decoder `Manual in
    let rec go seq =
      match decode decoder with
      | `Await -> begin
          match Seq.uncons seq with
          | Some (str, seq) ->
              src decoder str 0 (String.length str);
              go seq
          | None ->
              src decoder String.empty 0 0;
              go seq
        end
      | (`Header _ | `Copy _ | `Insert _) as value ->
          let next () = go seq in
          Seq.Cons (value, next)
      | `End -> Seq.Nil
      | `Malformed err -> failwith err
    in
    go seq
end

[@@@warning "-69"]

module N = struct
  type dst = [ `Buffer of Buffer.t | `Manual ]
  type encode = [ `Await | `Copy of int * int | `Insert of string | `End ]

  type encoder = {
      dst: dst
    ; src_len: int
    ; dst_len: int
    ; mutable o: Bstr.t
    ; mutable o_pos: int
    ; mutable o_max: int
    ; t: Bstr.t
    ; (* XXX(dinosaure): [bytes]? *)
      mutable t_pos: int
    ; mutable t_max: int
    ; mutable s: s
    ; mutable k: encoder -> encode -> [ `Ok | `Partial ]
  }

  and s = Header | Contents

  let o_rem e = e.o_max - e.o_pos + 1 [@@inline]

  let dst e s j l =
    if j < 0 || l < 0 || j + l > Bstr.length s then invalid_bounds j l s;
    e.o <- s;
    e.o_pos <- j;
    e.o_max <- j + l - 1

  let dst_rem encoder = o_rem encoder

  let partial k e = function
    | `Await -> k e
    | `Copy _ | `Insert _ | `End -> invalid_encode ()

  let flush k e =
    match e.dst with
    | `Manual ->
        e.k <- partial k;
        `Partial
    | `Buffer b ->
        (* XXX(dinosaure): optimize it! *)
        for i = 0 to e.o_pos - 1 do
          Buffer.add_char b (Bstr.get e.o i)
        done;
        e.o_pos <- 0;
        k e

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

  let t_range e max =
    e.t_pos <- 0;
    e.t_max <- max

  let rec t_flush k e =
    let blit e l =
      Bstr.blit e.t ~src_off:e.t_pos e.o ~dst_off:e.o_pos ~len:l;
      e.o_pos <- e.o_pos + l;
      e.t_pos <- e.t_pos + l
    in
    let rem = o_rem e in
    let len = e.t_max - e.t_pos + 1 in
    if rem < len then (
      blit e rem;
      flush (t_flush k) e)
    else (blit e len; k e)

  let rec encode_contents e v =
    let k e =
      e.k <- encode_contents;
      `Ok
    in
    match v with
    | `Await -> k e
    | `Copy (off, len) ->
        Logs.debug (fun m -> m "copy off:%d, len:%d" off len);
        let rem = o_rem e in
        let cmd = cmd off len in
        let required = 1 + M.required cmd in
        let s, j, k =
          if rem < required then (
            t_range e (required - 1);
            (e.t, 0, t_flush k))
          else
            let j = e.o_pos in
            e.o_pos <- e.o_pos + required;
            (e.o, j, k)
        in
        Bstr.set_uint8 s j (cmd lor 0x80);
        let pos = ref (j + 1) in
        let off = ref off in
        while !off <> 0 do
          if !off land 0xff != 0 then (Bstr.set_uint8 s !pos !off; incr pos);
          off := !off asr 8
        done;
        let len = ref len in
        while !len <> 0 do
          if !len land 0xff != 0 then (Bstr.set_uint8 s !pos !len; incr pos);
          len := !len asr 8
        done;
        k e
    | `Insert p ->
        Logs.debug (fun m -> m "insert len:%d" (String.length p));
        let rem = o_rem e in
        let required = 1 + String.length p in
        let s, j, k =
          if rem < required then (
            t_range e (required - 1);
            (e.t, 0, t_flush k))
          else
            let j = e.o_pos in
            e.o_pos <- e.o_pos + required;
            (e.o, j, k)
        in
        Bstr.set_uint8 s j (String.length p);
        Bstr.blit_from_string p ~src_off:0 s ~dst_off:(j + 1)
          ~len:(String.length p);
        k e
    | `End -> flush k e

  let store_variable_length buf off length =
    let l = ref length in
    let off = ref off in
    while !l >= 0x80 do
      Bstr.set_uint8 buf !off (!l lor 0x80 land 0xff);
      incr off;
      l := !l asr 7
    done;
    Bstr.set_uint8 buf !off !l

  let needed length =
    let l = ref length in
    let o = ref 0 in
    while !l >= 0x80 do
      incr o;
      l := !l asr 7
    done;
    incr o;
    !o
  [@@inline]

  let encode_header e v =
    let k e =
      e.k <- encode_contents (* XXX(dinosaure): short-cut [encode]. *);
      e.s <- Contents;
      e.k e v
    in
    let ndd = needed e.src_len + needed e.dst_len in
    let rem = o_rem e in
    (* assert (ndd <= 10) ; *)
    if rem >= ndd then (
      store_variable_length e.o e.o_pos e.src_len;
      store_variable_length e.o (e.o_pos + needed e.src_len) e.dst_len;
      e.o_pos <- e.o_pos + ndd;
      k e)
    else (
      t_range e ndd;
      store_variable_length e.t 0 e.src_len;
      store_variable_length e.t (needed e.src_len) e.dst_len;
      t_flush k e)

  let encode e v = e.k e v

  let encoder dst ~src_len ~dst_len =
    let o, o_pos, o_max =
      match dst with
      | `Manual -> (Bstr.empty, 1, 0)
      | `Buffer _ -> (Bstr.create De.io_buffer_size, 0, De.io_buffer_size - 1)
    in
    {
      dst
    ; src_len
    ; dst_len
    ; o
    ; o_pos
    ; o_max
    ; t= Bstr.create 128
    ; t_pos= 1
    ; t_max= 0
    ; s= Header
    ; k= encode_header
    }
end
