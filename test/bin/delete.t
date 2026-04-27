Prepare copies of bomb.pack
  $ carton index bomb.pack
  $ cp bomb.pack leaf.pack && cp bomb.idx leaf.idx && chmod u+w leaf.pack leaf.idx
  $ cp bomb.pack base.pack && cp bomb.idx base.idx && chmod u+w base.pack base.idx
  $ cp bomb.pack chain.pack && cp bomb.idx chain.idx && chmod u+w chain.pack chain.idx
  $ cp bomb.pack out.pack && cp bomb.idx out.idx && chmod u+w out.pack out.idx

Delete a leaf
  $ carton delete --pack leaf.pack 5faa3895522087022ba6fc9e64b02653bd7c4283
  $ carton verify -q leaf.pack
  $ carton get -q leaf.idx 5faa3895522087022ba6fc9e64b02653bd7c4283 -o missing > /dev/null 2>&1 || echo "not found"
  not found
  $ carton get -q leaf.idx 7af99c9e7d4768fa681f4fe4ff61259794cf719b -o leaf.commit
  $ carton get -q bomb.idx 7af99c9e7d4768fa681f4fe4ff61259794cf719b -o bomb.commit
  $ diff bomb.commit leaf.commit
  $ carton list leaf.pack | grep -c tombstone
  1
  $ carton verify leaf.pack | ocaml wc.ml
  17

Delete a base
  $ carton delete --pack base.pack 106d3b1c00034193bbe91194eb8a90fc45006377
  $ carton verify -q base.pack
  $ carton get -q base.idx 106d3b1c00034193bbe91194eb8a90fc45006377 -o missing > /dev/null 2>&1 || echo "not found"
  not found

  $ carton get -q base.idx c1971b07ce6888558e2178a121804774c4201b17 -o base.c1971b
  $ carton get -q bomb.idx c1971b07ce6888558e2178a121804774c4201b17 -o bomb.c1971b
  $ diff bomb.c1971b base.c1971b
  $ carton get -q base.idx d9513477b01825130c48c4bebed114c4b2d50401 -o base.d95134
  $ carton get -q bomb.idx d9513477b01825130c48c4bebed114c4b2d50401 -o bomb.d95134
  $ diff bomb.d95134 base.d95134

  $ carton list base.pack | grep -c tombstone
  3
  $ carton verify base.pack | ocaml wc.ml
  17

Delete multiple objects
  $ carton delete --pack chain.pack 106d3b1c00034193bbe91194eb8a90fc45006377
  $ carton delete --pack chain.pack dacaac6d3b2cf39ec8078dfb0bd3ce691e92557f
  $ carton verify -q chain.pack
  $ carton get -q chain.idx 106d3b1c00034193bbe91194eb8a90fc45006377 -o missing > /dev/null 2>&1 || echo "not found"
  not found
  $ carton get -q chain.idx dacaac6d3b2cf39ec8078dfb0bd3ce691e92557f -o missing > /dev/null 2>&1 || echo "not found"
  not found
  $ carton get -q chain.idx 7af99c9e7d4768fa681f4fe4ff61259794cf719b -o chain.commit
  $ diff bomb.commit chain.commit
  $ carton get -q chain.idx ad839baae5fc207ac0db1534ba4819cbb4a34bb9 -o chain.ad839ba
  $ carton get -q bomb.idx ad839baae5fc207ac0db1534ba4819cbb4a34bb9 -o bomb.ad839ba
  $ diff bomb.ad839ba chain.ad839ba

  $ carton list chain.pack | grep -c tombstone
  5
  $ carton verify chain.pack | ocaml wc.ml
  16
