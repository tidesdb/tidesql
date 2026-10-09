# Code Rules

TideSQL is a MariaDB storage engine around the TidesDB library, so these rules follow the library's
own `RULES.md` and adapt it where the server API decides how the code has to look.

1. **Simple control flow.** No `goto`, `setjmp`/`longjmp`, or exceptions as control flow. Recursion
   only with a stated depth bound. The WKB geometry parser is bounded by geometry nesting, and a
   foreign key cascade re-enters the handler and stops at `TDB_FK_MAX_CASCADE_DEPTH`.
2. **Bounded loops.** Every loop terminates by a stated bound or invariant, a count, the size of the
   data it walks, or the documented end of a server cursor or TidesDB iterator. Wait and retry loops
   carry an explicit maximum and a defined outcome when they reach it, as `tdb_retry_locked` does.
3. **Allocate deliberately.** Every allocation's failure is handled. Hot paths reuse handler-owned
   buffers sized once, a value the library hands back is freed by the code that read it, and nothing
   grows without bound; a buffer that could, such as the deferred keys of a bulk delete, carries a
   named cap.
4. **Smallest possible scope.** Declare data objects at the tightest scope that works.
5. **Check every return value.** Validate all function parameters; never ignore a non-void return.
   A library error is mapped through `tdb_rc_to_ha`, a transient `TDB_ERR_LOCKED` goes through the
   retry wrappers in `ha_tidesdb_internal.h`, and an iterator call that fails must not read as the
   end of the data. A result deliberately discarded is cast to `(void)` with the reason beside it.
6. **Minimal preprocessor use.** Macros limited to file inclusion and simple constants, no token
   pasting. Conditional compilation only for server version differences (`MYSQL_VERSION_ID`),
   optional server features (`WITH_WSREP`, `WITH_PARTITION_STORAGE_ENGINE`) and platform or compiler
   differences, kept at the boundary that needs it.
7. **Restricted pointer use.** Function pointers only where the server or the library takes them,
   the handler and handlerton hooks, the full-text interface, system variable callbacks and library
   commit hooks. Pointer-to-pointer only for out-parameters and arrays of handles.
8. **Zero-warning compilation.** All warnings enabled, all warnings fixed, on every server version
   and platform in the CI matrix, and the code passes static analysis clean before release.
9. **No magic numbers or strings.** Every literal with meaning gets a named constant in
   `ha_tidesdb_constants.h` instead of a bare number or string appearing inline.
10. **Testable by design.** Logic that needs no server lives in `src/core` and is unit tested under
    `test/`. Engine behavior is tested with MTR in `mysql-test/tidesdb`, with `tidesdb_galera` and
    `tidesdb_rpl` for replication. Every bug fix lands with an MTR case that fails without it, and a
    case compares against InnoDB or a forced table scan wherever the two should agree.
11. **Comments explain why, in flowing prose.** No `Label: value` patterns. `src/core` and `test/`
    keep the library's lowercase style; handler code may use sentence case.
12. **Prove it before committing.** Run the core unit tests with ASAN and UBSAN, and build the plugin
    and run the `tidesdb`, `tidesdb_galera` and `tidesdb_rpl` suites on every MariaDB version in the
    CI matrix, currently 11.4 and 13. A server-free change also runs with TSAN where it is threaded.
13. **Files stay under 1000 lines.** Source and header files in `tidesdb/` and `src/` are kept under
    **1000** lines. Tests are not held to this. A few files are graced and listed below; a graced
    file still has a hard ceiling, and nothing else may exceed 1000 lines without being added here.

    | File | Ceiling | Why |
    | --- | --- | --- |
    | `ha_tidesdb.h` | 1400 | It declares the whole `ha_tidesdb` handler class, whose members the server requires on one class, and the share it reads from. Splitting it would scatter one type across headers. |
    | `ha_tidesdb.cc` | 1300 | The system variables are file-static and listed in the plugin declaration in the same translation unit, so they cannot move out of it. |
14. **Functions stay under 100 lines.** Functions in `tidesdb/` and `src/` should be no greater than
    **100** lines.

## Documentation Style

Comments should explain *why* and *what for*, not restate the code. Skip comments that just repeat a
variable or type name. Every public struct and function in `src/core` gets a doc comment in this
format, the same as the library's.

### Structs

```c
/**
 * mbr_t
 * an axis-aligned bounding rectangle, the unit every spatial key, value and predicate works in
 * @param xmin the smallest x the geometry reaches
 * @param ymin the smallest y the geometry reaches
 * @param xmax the largest x the geometry reaches
 * @param ymax the largest y the geometry reaches
 */
```

### Functions

```c
/**
 * mbr_predicate
 * test a stored row rectangle against a query rectangle under the requested spatial match
 * @param mode the spatial relation to test; unsupported always fails
 * @param query the query rectangle
 * @param entry the stored row rectangle
 * @return true when entry relates to query as mode requires
 */
```

**Conventions:**

- First line: the identifier name.
- Second line: one-sentence purpose, lowercase, no trailing period.
- `@param` / `@name`-style fields: one line per parameter or struct field, stating type constraints and nullability where relevant.
- `@return`: what each outcome means, not just "returns int."
- No inline comments duplicating the doc comment's information inside the function body.

Handler code documents a member where it is declared in `ha_tidesdb.h` or the unit's header, in prose
that says what the member is for and what it returns.
