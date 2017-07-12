#------------------------------------------------------------------------------
# Makefile
#
# Simple makefile for installing the schema.
#------------------------------------------------------------------------------
.PHONY: all install clean

# Inherit from upper-level make.config
TOP = ..
include $(TOP)/make.config

SCHEMAS = ard_metadata_v1_0.xsd

all:

install:
	install -d $(schema_link_path)
	install -d $(schema_lib_install_path)
	install -m 644 $(SCHEMAS) $(schema_lib_install_path)
	@for schema in $(SCHEMAS); do \
        cmd="ln -sf $(schema_lib_link_path)/$$schema $(schema_link_path)/$$schema"; \
        echo "$$cmd"; $$cmd; \
        done

clean:
