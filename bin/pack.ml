open Cmdliner

let default =
  let open Term in
  ret (const (`Help (`Pager, None)))

let () =
  let doc = "A tool to manipulate PACKv2 files." in
  let man = [] in
  let info = Cmd.info "carton" ~doc ~man in
  let cmd =
    Cmd.group ~default info
      [
        Apply.cmd; Diff.cmd; Explode.cmd; Get.cmd; Index.cmd; List.cmd; Make.cmd
      ; Verify.cmd; Merge.cmd
      ]
  in
  Cmd.(exit @@ eval_result cmd)
