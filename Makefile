all: qsome.lua qsome-lib.lua

qless-core/qless-lib.lua: qless-core/*
	make -C qless-core qless-lib.lua

qsome-lib.lua: base.lua job.lua queue.lua qless-core/qless-lib.lua
	cat {qless-core/qless-lib,base,job,queue}.lua > qsome-lib.lua

qsome.lua: qsome-lib.lua api.lua
	# Cat these files out, but remove all the comments from the source
	cat {qsome-lib,api}.lua | \
		egrep -v '^[[:space:]]*--[^\[]' | \
		egrep -v '^--$$' > qsome.lua

clean:
	rm -f qsome{,-lib}.lua qless-core/qless{,-lib}.lua

.PHONY: test
test: qsome.lua
	nosetests --exe -v
