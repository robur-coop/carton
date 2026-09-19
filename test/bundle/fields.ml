let rec go () =
  match input_line stdin with
  | exception End_of_file -> ()
  | line ->
      let fields = String.split_on_char ' ' line in
      let fields = List.filter (( <> ) "") fields in
      begin match fields with
      | a :: b :: _ -> print_endline (a ^ " " ^ b)
      | _ -> print_endline line
      end;
      go ()

let () = go ()
