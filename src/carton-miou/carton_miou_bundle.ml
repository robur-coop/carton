let ( let* ) = Result.bind

let save filename seq =
  let oc = open_out_bin (Fpath.to_string filename) in
  let finally () = close_out oc in
  Fun.protect ~finally @@ fun () -> Seq.iter (output_string oc) seq

let of_packs ~cfg ~digest ~sort ?level hdr filenames =
  let pack = Carton_miou_unix.merge ~cfg ~digest ~sort ?level filenames in
  Bundle.to_seq hdr pack

let read ?ref_length filename =
  let seq = Carton_miou_unix.seq_of_filename filename in
  Bundle.split ?ref_length seq

let split ?ref_length filename ~pack =
  let* hdr, seq = read ?ref_length filename in
  save pack seq; Ok hdr
