const xsjs = require('@sap/xsjs');
const xsenv = require('@sap/xsenv');

var options = {
	// anonymous: false,   // with UAA
	anonymous: true,   // without UAA
	xsApplicationUser: true,
	auditLog: { logToConsole: true },
	redirectUrl: '/jobs/index.xsjs',
	headers: {"access-control-allow-origin": "*"}
};

try {
	options = Object.assign(options, xsenv.getServices({ hana: { tag: "hana" } }));
} catch (err) {
	console.log("[WARN]", err.message);
}

/*try {
	options = Object.assign(options, xsenv.getServices({ uaa: { tag: "xsuaa" } }));
} catch (err) {
	console.log("[WARN]", err.message);
}*/

const port = process.env.PORT || 5001;
xsjs(options).listen(port);
console.info('Listening on http://localhost:' + port);
