Generate bomb.idx from bomb.pack
  $ carton index bomb.pack
Verify bomb.pack from bomb.idx
  $ carton verify --without-progress --without-consumed bomb.idx > idx.out
Verify bomb.pack
  $ carton verify --without-progress --without-consumed bomb.pack > pack.out
Compare the output of verifications
  $ diff idx.out pack.out
Get an object from bomb.pack via a hash
  $ carton get -q bomb.idx 7af99c9e7d4768fa681f4fe4ff61259794cf719b -o 01.commit
Get the same object from bomb.pack via an offset
  $ carton get -q bomb.pack 0xc -o 02.commit
Compare the output of objects
  $ diff 01.commit 02.commit
Explode bomb.pack to files
  $ carton explode "bomb/%s/%s" bomb.pack > entries.pack
Re-pack bomb.pack to a new new.pack
  $ carton make --number 18 --entries entries.pack new.pack
Generate new.idx from new.pack
  $ carton index new.pack
Verify new.pack from new.idx
  $ carton verify --without-progress --without-consumed new.idx > idx.out
Verify new.pack
  $ carton verify --without-progress --without-consumed new.pack > pack.out
Compare the output of verifications
  $ diff idx.out pack.out
Get an object from new.pack via a hash
  $ carton get -q new.idx 7af99c9e7d4768fa681f4fe4ff61259794cf719b -o 03.commit
Compare the output of objects
  $ diff 02.commit 03.commit
  $ carton merge -p bomb.pack -p bomb.pack merge.pack
  $ carton index merge.pack
  $ carton verify merge.pack -q
  $ carton get -q merge.idx 7af99c9e7d4768fa681f4fe4ff61259794cf719b -o 04.commit
  $ diff 03.commit 04.commit
