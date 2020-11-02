## TODO
- ReorgPageFinder
  - Add fp_check after every reorg findings
  - Remove fragments
  - Change all ReorgPageFinder collection to fable (Probably define some macro)

- Accuracy
  - Wayback_alias with urls in the same dir comp
  - Title init, more separation
  - Link signature, requires total uniqueness

- Efficiency
  - Text comparison. Introduce k-shingling for pruning
  - (Consider parent picking)

- Logging
  - Consider log all backpath finding process (currently only the final found path is logged)