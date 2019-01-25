add_executable(cdc-replicator main.cc replicator.cc table.cc sql.cc executor.cc)
target_link_libraries(cdc-replicator mcsapi maxscale-common)
install_executable(cdc-replicator cdc-replicator)

if (WITH_SCRIPTS)
  install_custom_file(cdc-replicator.service ${MAXSCALE_CONFDIR}/systemd/system/ cdc-replicator)
endif()
