NODE = node
NPM = npm
JSDOC = jsdoc
name = all

#
# Generate the ES5 versions of the files
#
generate:
	./node_modules/.bin/babel --presets="es2015" lib/config_servers.js -o es5/config_servers.js
	./node_modules/.bin/babel --presets="es2015" lib/logger.js -o es5/logger.js
	./node_modules/.bin/babel --presets="es2015" lib/mongos.js -o es5/mongos.js
	./node_modules/.bin/babel --presets="es2015" lib/replset.js -o es5/replset.js
	./node_modules/.bin/babel --presets="es2015" lib/server.js -o es5/server.js
	./node_modules/.bin/babel --presets="es2015" lib/sharded.js -o es5/sharded.js
