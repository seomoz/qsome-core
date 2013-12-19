all: qsome.lua qsome-lib.lua

qless-core/qless-lib.lua: qless-core/Makefile
	make -C qless-core qless-lib.lua

qsome-lib.lua: qless-core/qless-lib.lua base.lua job.lua queue.lua
	cat qless-core/qless-lib.lua base.lua job.lua queue.lua > qsome-lib.lua

qsome.lua: qsome-lib.lua api.lua
	# Cat these files out, but remove all the comments from the source
	cat qsome-lib.lua api.lua | \
		egrep -v '^[[:space:]]*--[^\[]' | \
		egrep -v '^--$$' > qsome.lua

clean:
	rm -f qsome*.lua qless-core/qless*.lua

.PHONY: test
test: qsome.lua
	nosetests --exe -v
