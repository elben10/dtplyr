# dtplyr 0.0.2.9000

- `inner_join()`, `left_join()`, `right_join()`, and `full_join()`: new `suffix` 
  argument which allows you to control what suffix duplicated variable names 
  receive, as introduced in dplyr 0.5 (#40, @christophsax).

- Joins use extended `merge.data.table()` and the `on` argument, introduced in
  data.table 1.9.6. Avoids copy and allows joins by different keys (#20, #21,
  @christophsax).


# dtplyr 0.0.2

- This is a compatibility release. It makes dtplyr compatible with
  dplyr 0.6.0 in addition to dplyr 0.5.0.


# dtplyr 0.0.1

- `distinct()` gains `.keep_all` argument (#30, #31).

- Slightly improve test coverage (#6).

- Install `devtools` from GitHub on Travis (#32).

- Joins return `data.table`. Right and full join are now implemented (#16, #19).

- Remove warnings from tests (#4).

- Extracted from `dplyr` at revision e5f2952923028803.
