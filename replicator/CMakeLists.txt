add_library(replicator SHARED replicator.cc table.cc sql.cc)
target_link_libraries(replicator mcsapi maxscale-common)
set_target_properties(replicator PROPERTIES VERSION "1.0.0"  LINK_FLAGS -Wl,-z,defs)
install_module(replicator cdc-replicator)

add_executable(debug-replicator debug.cc)
target_link_libraries(debug-replicator replicator)
install_executable(debug-replicator cdc-replicator)
