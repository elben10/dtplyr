
.onLoad <- function(libname, pkgname) {
  if (utils::packageVersion("dplyr") > "0.5.0") {
    register_s3_method("dplyr", "do", "data.table")
    register_s3_method("dplyr", "group_by", "data.table")
    register_s3_method("dplyr", "distinct", "data.table")
    register_s3_method("dplyr", "filter", "data.table")
    register_s3_method("dplyr", "summarise", "data.table")
    register_s3_method("dplyr", "mutate", "data.table")
    register_s3_method("dplyr", "arrange", "data.table")
    register_s3_method("dplyr", "select", "data.table")
    register_s3_method("dplyr", "rename", "data.table")
    register_s3_method("dplyr", "slice", "data.table")
  }

  register_s3_method("dplyr", "do_", "data.table")
  register_s3_method("dplyr", "do_", "tbl_dt")

  register_s3_method("base", "print", "grouped_dt")
  register_s3_method("dplyr", "groups", "grouped_dt")
  register_s3_method("dplyr", "group_size", "grouped_dt")
  register_s3_method("dplyr", "n_groups", "grouped_dt")
  register_s3_method("dplyr", "ungroup", "grouped_dt")

  register_s3_method("dplyr", "group_by_", "data.table")
  register_s3_method("dplyr", "do_", "grouped_dt")
  register_s3_method("dplyr", "distinct_", "grouped_dt")

  register_s3_method("dplyr", "inner_join", "data.table")
  register_s3_method("dplyr", "left_join", "data.table")
  register_s3_method("dplyr", "right_join", "data.table")
  register_s3_method("dplyr", "semi_join", "data.table")
  register_s3_method("dplyr", "anti_join", "data.table")
  register_s3_method("dplyr", "full_join", "data.table")

  register_s3_method("dplyr", "sample_n", "tbl_dt")
  register_s3_method("dplyr", "sample_n", "grouped_dt")
  register_s3_method("dplyr", "sample_frac", "tbl_dt")
  register_s3_method("dplyr", "sample_frac", "grouped_dt")

  register_s3_method("dplyr", "distinct_", "data.table")
  register_s3_method("dplyr", "distinct_", "tbl_dt")

  register_s3_method("dplyr", "filter_", "grouped_dt")
  register_s3_method("dplyr", "filter_", "tbl_dt")
  register_s3_method("dplyr", "filter_", "data.table")

  register_s3_method("dplyr", "summarise_", "grouped_dt")
  register_s3_method("dplyr", "summarise_", "tbl_dt")
  register_s3_method("dplyr", "summarise_", "data.table")

  register_s3_method("dplyr", "mutate_", "grouped_dt")
  register_s3_method("dplyr", "mutate_", "tbl_dt")
  register_s3_method("dplyr", "mutate_", "data.table")

  register_s3_method("dplyr", "arrange_", "grouped_dt")
  register_s3_method("dplyr", "arrange_", "tbl_dt")
  register_s3_method("dplyr", "arrange_", "data.table")

  register_s3_method("dplyr", "select_", "grouped_dt")
  register_s3_method("dplyr", "select_", "tbl_dt")
  register_s3_method("dplyr", "select_", "data.table")

  register_s3_method("dplyr", "rename_", "grouped_dt")
  register_s3_method("dplyr", "rename_", "tbl_dt")
  register_s3_method("dplyr", "rename_", "data.table")

  register_s3_method("dplyr", "slice_", "grouped_dt")
  register_s3_method("dplyr", "slice_", "tbl_dt")
  register_s3_method("dplyr", "slice_", "data.table")

  invisible()
}

register_s3_method <- function(pkg, generic, class, fun = NULL) {
  stopifnot(is.character(pkg), length(pkg) == 1)
  envir <- asNamespace(pkg)

  stopifnot(is.character(generic), length(generic) == 1)
  stopifnot(is.character(class), length(class) == 1)
  if (is.null(fun)) {
    fun <- get(paste0(generic, ".", class), envir = parent.frame())
  }
  stopifnot(is.function(fun))


  if (pkg %in% loadedNamespaces()) {
    registerS3method(generic, class, fun, envir = envir)
  }

  # Always register hook in case package is later unloaded & reloaded
  setHook(
    packageEvent(pkg, "onLoad"),
    function(...) {
      registerS3method(generic, class, fun, envir = envir)
    }
  )
}
