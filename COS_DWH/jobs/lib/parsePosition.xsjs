function AuditInitialize(process) {
	var audit_id = 1;
	try {
		//get max ID of Audit table
		var dbCon = $.db.getConnection();
		var query = 'SELECT MAX(AUDIT_ID) FROM COS_DWH.ADM_AUDIT';
		var pstmt = dbCon.prepareStatement(query);
		var rs = pstmt.executeQuery();
		if (rs.next() !== null) {
			audit_id = rs.getInteger(1) + 1;
		}
		dbCon.close();
		//Insert start time of current job into Audit Table
		var now = new Date();
		var dbConInsert = $.db.getConnection();
		var st = dbConInsert.prepareStatement("INSERT INTO \"COS_DWH\".\"ADM_AUDIT\" VALUES (?,?,?,?,?,?)");
		st.setInt(1, audit_id);
		st.setString(2, process);
		st.setTimestamp(3, now);
		st.setNull(4);
		st.setString(5, 'RUNNING');
		st.setNull(6);

		st.executeQuery();
		dbConInsert.commit();
		dbConInsert.close();
		return audit_id;
	} catch (e) {
		$.response.setBody("Audit Initialize - " + e.message);
	}
}

function AuditFinalize(audit_id) {
	var status = '';
	try {
		//get max ID of Audit table
		var dbCon = $.db.getConnection();
		var query = 'SELECT PROCESS_STATUS FROM COS_DWH.ADM_AUDIT WHERE AUDIT_ID = ' + audit_id;
		var pstmt = dbCon.prepareStatement(query);
		var rs = pstmt.executeQuery();
		if (rs.next() !== null) {
			status = rs.getString(1);
		}
		dbCon.close();

		if (status === 'RUNNING') {
			//Insert start time of current job into Audit Table if there has been no error during the execution
			var now = new Date();
			var dbConInsert = $.db.getConnection();
			var st = dbConInsert.prepareStatement("UPDATE \"COS_DWH\".\"ADM_AUDIT\" SET \"END_DATE\"=?, \"PROCESS_STATUS\"=? WHERE \"AUDIT_ID\"=?");
			st.setTimestamp(1, now);
			st.setString(2, 'FINISHED');
			st.setInt(3, audit_id);

			st.executeQuery();
			dbConInsert.commit();
			dbConInsert.close();
		}
	} catch (e) {
		$.response.setBody("Audit Finalize - " + e.message);
	}
}

function AuditError(audit_id, section, error) {
	//exclude warnings that are not errors due to JSON parsing
	if (error !== "parseAttempt[i] is undefined" && error !== "response.body is undefined") {
		try {
			//Insert start time of current job into Audit Table
			var now = new Date();
			var dbConInsert = $.db.getConnection();
			var st = dbConInsert.prepareStatement(
				"UPDATE \"COS_DWH\".\"ADM_AUDIT\" SET END_DATE=?, PROCESS_STATUS=?,DESCRIPTION=? WHERE AUDIT_ID=?");
			st.setTimestamp(1, now);
			st.setString(2, 'ERROR');
			st.setString(3, section + ' == ' + error);
			st.setInt(4, audit_id);

			st.executeQuery();
			dbConInsert.commit();
			dbConInsert.close();
		} catch (e) {
			$.response.setBody("Audit Error - " + e.message);
		}
	}
}

function parsePosition() {

    var auditId = AuditInitialize('Position');

	var dest = $.net.http.readDestination("SSFF");
	var iterationToken = '';
	var conn = $.db.getConnection();
	var queryTruncate = "TRUNCATE TABLE \"COS_DWH\".\"STG_POSITION\" ";
	var st = conn.prepareStatement(queryTruncate);
	st.execute();

	do {
		var client = new $.net.http.Client();
		var TokenReq = new $.web.WebRequest($.net.http.GET, '/Position?' + iterationToken + '$format=json&fromDate=1900-01-01&$select=code,createdDateTime,effectiveStartDate,effectiveEndDate,vacant,company,costCenter,businessUnit,division,department,cust_subdepartment,externalName_defaultValue');
		TokenReq.headers.set('Authorization', 'Basic QURNSU5DT1NFTlRJTk9AQzAwMDM1NTgwNTBQOkNvc2VudGlubzE3');

		client.request(TokenReq, dest);
		var response = client.getResponse();
		client.close();

		conn = $.db.getConnection();
		var content = JSON.parse(response.body.asString());
		var parseAttempt = content.d.results;
		var toStr = "";

		var iterationTokenString = content.d.__next;
		if (iterationTokenString === null || iterationTokenString === undefined || iterationTokenString === 'null') {
			iterationToken = '';
		} else {
			iterationToken = iterationTokenString.substring(iterationTokenString.indexOf('$skiptoken='), iterationTokenString.length) +
				'&';
		}

		try {
			for (var i = 0; i < parseAttempt.length; i++) {

				var code = parseAttempt[i].code + toStr;
				var createdDateTime = parseAttempt[i].createdDateTime + toStr;
				var effectiveStartDate = parseAttempt[i].effectiveStartDate + toStr;
				var effectiveEndDate = parseAttempt[i].effectiveEndDate + toStr;
				var vacant = parseAttempt[i].vacant + toStr;
				var company = parseAttempt[i].company + toStr;
				var costCenter = parseAttempt[i].costCenter + toStr;
				var businessUnit = parseAttempt[i].businessUnit + toStr;
				var division = parseAttempt[i].division + toStr;
				var department = parseAttempt[i].department + toStr;
				var subDepartment = parseAttempt[i].cust_subdepartment + toStr;
				var externalName = parseAttempt[i].externalName_defaultValue + toStr;

				var queryInsert = "INSERT INTO \"COS_DWH\".\"STG_POSITION\" VALUES (?,?,?,?,?,?,?,?,?,?,?,?)";
				st = conn.prepareStatement(queryInsert);

				//handle Strings
				st.setString(1, code);
				st.setString(2, company);
				st.setString(5, vacant);
				st.setString(6, company);
				st.setString(7, costCenter);
				st.setString(8, businessUnit);
				st.setString(9, division);
				//Change null values to a more friendly value for front-end
				if ( department === null || department === undefined || department === 'null' || department === 'undefined' ) {
				    department = 'Sin Asignar';
				}
				st.setString(10, department);
				//Change null values to a more friendly value for front-end
				if ( subDepartment === null || subDepartment === undefined || subDepartment === 'null' || subDepartment === 'undefined' ) {
				    subDepartment = 'Sin Asignar';
				}
				st.setString(11, subDepartment);
				st.setString(12, externalName);

				//handle Dates
				if (createdDateTime === null || createdDateTime === undefined || createdDateTime === 'null') {
					st.setDate(2, new Date(0));
				} else {
					createdDateTime = new Date(eval(createdDateTime.substring(6, createdDateTime.length - 7)));
					st.setDate(2, createdDateTime);
				}
				
				if (effectiveStartDate === null || effectiveStartDate === undefined || effectiveStartDate === 'null') {
					st.setDate(3, new Date(0));
				} else {
					effectiveStartDate = new Date(eval(effectiveStartDate.substring(6, effectiveStartDate.length - 2)));
					st.setDate(3, effectiveStartDate);
				}
				
				if (effectiveEndDate === null || effectiveEndDate === undefined || effectiveEndDate === 'null') {
					st.setDate(4, new Date(0));
				} else {
					effectiveEndDate = new Date(eval(effectiveEndDate.substring(6, effectiveEndDate.length - 2)));
					st.setDate(4, effectiveEndDate);
				}
				st.execute();
				conn.commit();
			}
			conn.close();
		} catch (e) {
			$.response.setBody("Audit " + createdDateTime + " " + effectiveStartDate + " " + effectiveEndDate );
			AuditError(auditId, 'Position', e.message);
			return;
		}
	}
	//will iterate at least once for the case of no token provided
	while (iterationToken !== '')
	AuditFinalize(auditId);
}
//parsePosition();