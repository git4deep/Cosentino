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

function parseJobApplication() {

    var auditId = AuditInitialize( 'jobApplication');
	var dest = $.net.http.readDestination("SSFF");
	var top = 100;
	var skip = 0;
	var response;
	var content;
	var conn = $.db.getConnection();
	var parseAttempt;
	var queryTruncate = "TRUNCATE TABLE \"COS_DWH\".\"STG_JOB_APPLICATION\" ";
	var st = conn.prepareStatement(queryTruncate);
	st.execute();
	var client = new $.net.http.Client();
	//get total number of records
	var TokenReq = new $.web.WebRequest($.net.http.GET, '/JobApplication/$count');
	TokenReq.headers.set('Authorization', 'Basic QURNSU5DT1NFTlRJTk9AQzAwMDM1NTgwNTBQOkNvc2VudGlubzE3');
	client.request(TokenReq, dest);
	var recordsToLoad = client.getResponse().body.asString();

	do {
		//start getting records in batches
		TokenReq = new $.web.WebRequest($.net.http.GET, '/JobApplication?$expand=jobAppStatus&$format=json&$fromDate=1900-01-01&$top=' + top + '&$skip=' + skip +
			'&$select=appStatusSetItemId,applicationId,applicationTemplateId,candidateId,department,firstName,hiredOn,jobAppGuid,jobReqId,jobAppStatus'
		);
		TokenReq.headers.set('Authorization', 'Basic QURNSU5DT1NFTlRJTk9AQzAwMDM1NTgwNTBQOkNvc2VudGlubzE3');

		try {
			client.request(TokenReq, dest);
			response = client.getResponse();
			content = JSON.parse(response.body.asString());
			parseAttempt = content.d.results;

		} catch (e) {
			client.close();
			continue;
			//throw new Error("Failed to get response from the server!" + e.message);			//TODO: Audit this error
		} finally {
			client.close();
		}

		var toStr = "";

		try {
			for (var i = 0; i < parseAttempt.length; i++) {

				var appStatusSetItemId = parseAttempt[i].appStatusSetItemId + toStr;
				var applicationId = parseAttempt[i].applicationId + toStr;
				var applicationTemplateId = parseAttempt[i].applicationTemplateId + toStr;
				var candidateId = parseAttempt[i].candidateId + toStr;
				//var dateOfBirth = parseAttempt[i].dateOfBirth + toStr;
				var dateOfBirth;
				var department = parseAttempt[i].department + toStr;
				var firstName = parseAttempt[i].firstName + toStr;
				//var gender = parseAttempt[i].gender + toStr;
				var gender = "";
				var hiredOn = parseAttempt[i].hiredOn + toStr;
				var jobAppGuid = toStr;
				//can be null
				if(parseAttempt[i].jobAppStatus){
				    jobAppGuid = parseAttempt[i].jobAppStatus.appStatusName + toStr;
				}
				var jobReqId = parseAttempt[i].jobReqId + toStr;

				var queryInsert = "INSERT INTO \"COS_DWH\".\"STG_JOB_APPLICATION\" VALUES (?,?,?,?,?,?,?,?,?,?,?)";
				st = conn.prepareStatement(queryInsert);

				st.setString(6, department);
				st.setString(7, firstName);
				st.setString(8, gender);
				
				//actual column in Database is job applicationStatus
				st.setString(10, jobAppGuid);
				//handle Dates
                
				if (dateOfBirth === null || dateOfBirth === undefined || dateOfBirth === 'null') {
					st.setDate(5, new Date(0));
				} else {
					//Luis 25.03.2021
					//dateOfBirth = new Date(eval(dateOfBirth.substring(6, dateOfBirth.length - 7)));
					dateOfBirth = new Date(1970, 1, 1);
					st.setDate(5, dateOfBirth);
				}

				if (hiredOn === null || hiredOn === undefined || hiredOn === 'null') {
					st.setDate(9, new Date(0));
				} else {
					//Luis 25.03.2021
					//hiredOn = new Date(eval(hiredOn.substring(6, hiredOn.length - 7)));
					hiredOn = new Date(1970, 1, 1);
					st.setDate(9, hiredOn);
				}

				//handle Integers
				if (appStatusSetItemId === null || appStatusSetItemId === undefined || appStatusSetItemId === 'null') {
					st.setInt(1, 0);
				} else {
					st.setInt(1, parseFloat(appStatusSetItemId));
				}

				if (applicationId === null || applicationId === undefined || applicationId === 'null') {
					st.setInt(2, 0);
				} else {
					st.setInt(2, parseFloat(applicationId));
				}

				if (applicationTemplateId === null || applicationTemplateId === undefined || applicationTemplateId === 'null') {
					st.setInt(3, 0);
				} else {
					st.setInt(3, parseFloat(applicationTemplateId));
				}

				if (candidateId === null || candidateId === undefined || candidateId === 'null') {
					st.setInt(4, 0);
				} else {
					st.setInt(4, parseFloat(candidateId));
				}

				if (jobReqId === null || jobReqId === undefined || jobReqId === 'null') {
					st.setInt(11, 0);
				} else {
					st.setInt(11, parseFloat(jobReqId));
				}
				st.execute();
				conn.commit();
			}

		} catch (e) {
		    AuditError(auditId,'Job Application', "Audit Initialize Job Application - " + e.message + " | " + skip + " | " + top + " | " + i + " | " + jobReqId + " | " +
				candidateId + " | " + applicationTemplateId + " | " + applicationId + " | " + appStatusSetItemId + " | " + hiredOn + " | " +
				dateOfBirth);
			return;
		}
		skip += top;
	}
	//will iterate at least once for the first 100 rows
	while (recordsToLoad > skip);
	conn.close();
	AuditFinalize(auditId);
}
//parseJobApplication();