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

function parseEmpPayRecurring() {
    var auditID = AuditInitialize('Emp Pay Recurring');
	var dest = $.net.http.readDestination("SSFF");
	var iterationToken = '';
	var conn = $.db.getConnection();
	var queryTruncate = "DELETE FROM \"COS_DWH\".\"STG_EMP_PAY_COMP_RECURRING\" ";
	var st = conn.prepareStatement(queryTruncate);
	st.execute();

	do {
		var client = new $.net.http.Client();
		var TokenReq = new $.web.WebRequest($.net.http.GET, '/EmpPayCompRecurring?' + iterationToken + '$format=json&fromDate=1900-01-01');
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
                
                var userId = parseAttempt[i].userId + toStr;
                var startDate = parseAttempt[i].startDate + toStr;
				var endDate = parseAttempt[i].endDate + toStr;
				var frequency =  parseAttempt[i].frequency + toStr;
				var payCompValue =  parseAttempt[i].paycompvalue + toStr;
				var currencyCode =  parseAttempt[i].currencyCode + toStr;
				var payComponent =  parseAttempt[i].payComponent + toStr;
				var seqNumber    =  parseAttempt[i].seqNumber + toStr;

				var queryInsert = "INSERT INTO \"COS_DWH\".\"STG_EMP_PAY_COMP_RECURRING\" VALUES (?,?,?,?,?,?,?,?)";
				st = conn.prepareStatement(queryInsert);

				//handle Strings
				st.setString(1, userId);
				st.setString(4, frequency);
				st.setString(6, currencyCode);
				st.setString(7, payComponent);

				//handle Dates
				if (startDate === null || startDate === undefined || startDate === 'null') {
					st.setTimestamp(2, new Date(0));
				} else {
					//Luis 25.03.2021
					//startDate = new Date(eval(startDate.substring(6, startDate.length - 2)));
					startDate = new Date(1990, 1, 1);
					st.setTimestamp(2, startDate);
				}
				
				if (endDate === null || endDate === undefined || endDate === 'null') {
					st.setTimestamp(3, new Date(0));
				} else {
					//Luis 25.03.2021
					//endDate = new Date(eval(endDate.substring(6, endDate.length - 2)));
					endDate = new Date(2029, 1, 1);
					st.setTimestamp(3, endDate);
				}

				//handle Doubles / Integers
				if (payCompValue === null || payCompValue === undefined || payCompValue === 'null') {
					st.setDouble(5, 0);
				} else {
					st.setDouble(5, parseFloat(payCompValue));
				}
				if (seqNumber === null || seqNumber === undefined || seqNumber === 'null') {
					st.setDouble(8, 0);
				} else {
					st.setDouble(8, parseFloat(seqNumber));
				}
				
				st.execute();
				conn.commit();
			}
			conn.close();
		} catch (e) {
			//$.response.setBody("Audit Initialize - " + e.message);
			AuditError(auditID, 'Emp Pay Recurring', e.message);
		}
	}
	//will iterate at least once for the case of no token provided
	while (iterationToken !== '')
	AuditFinalize(auditID);
}
//parseEmpPayRecurring();