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

function parseEmpJob() {

    var auditId = AuditInitialize('Emp Job');
    //console.log("auditId"+auditId);

	var dest = $.net.http.readDestination("SSFF");
    //console.log("dest"+dest);
	var iterationToken = '';
	var conn = $.db.getConnection();
	var queryTruncate = "TRUNCATE TABLE \"COS_DWH\".\"STG_EMP_JOB\" ";
	var st = conn.prepareStatement(queryTruncate);
	st.execute();
    //console.log("beforeloop");

	do {
	    try {
		var client = new $.net.http.Client();
		var TokenReq = new $.web.WebRequest($.net.http.GET, '/EmpJob?' + iterationToken + '$format=json&fromDate=1900-01-01');
		TokenReq.headers.set('Authorization', 'Basic QURNSU5DT1NFTlRJTk9AQzAwMDM1NTgwNTBQOkNvc2VudGlubzE3');

		client.request(TokenReq, dest);
        console.log("before response");
		var response = client.getResponse();
        //console.log("afterresponse"+response.body.asString());
        //console.log("after response"+response + JSON.Stringify(response));
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
        //console.log("iterationToken"+iterationToken);
        //console.log(parseAttempt.length+"parseattempt");
		
			for (var i = 0; i < parseAttempt.length; i++) {

				var businessUnit = parseAttempt[i].businessUnit + toStr;
				var company = parseAttempt[i].company + toStr;
				var costCenter = parseAttempt[i].costCenter + toStr;
				var standardJobCode = parseAttempt[i].customString12 + toStr;
				var subDepartment = parseAttempt[i].customString15 + toStr;
				var department = parseAttempt[i].department + toStr;
				var emplStatus = parseAttempt[i].emplStatus + toStr;
				var employeeType = parseAttempt[i].employeeType + toStr;
				var employmentType = parseAttempt[i].employmentType + toStr;
				var endDate = parseAttempt[i].endDate + toStr;
				var event = parseAttempt[i].event + toStr;
				var eventReason = parseAttempt[i].eventReason + toStr;
				var holidayCalendarCode = parseAttempt[i].holidayCalendarCode + toStr;
				var isFulltimeEmployee = parseAttempt[i].isFulltimeEmployee + toStr;
				var jobCode = parseAttempt[i].jobCode + toStr;
				var jobTitle = parseAttempt[i].jobTitle + toStr;
				var location = parseAttempt[i].location + toStr;
				var managerId = parseAttempt[i].managerId + toStr;
				var position = parseAttempt[i].position + toStr;
				var positionEntryDate = parseAttempt[i].positionEntryDate + toStr;
				var regularTemp = parseAttempt[i].regularTemp + toStr;
				var retired = parseAttempt[i].retired + toStr;
				var standardHours = parseAttempt[i].standardHours + toStr;
				var startDate = parseAttempt[i].startDate + toStr;
				var userId = parseAttempt[i].userId + toStr;
				var workerCategory = parseAttempt[i].workerCategory + toStr;
				var workingDaysPerWeek = parseAttempt[i].workingDaysPerWeek + toStr;
				var workscheduleCode = parseAttempt[i].workscheduleCode + toStr;
				var division = parseAttempt[i].division + toStr;
				var occupationPercent = parseAttempt[i].customDouble11 + toStr;
				var personalDivision = parseAttempt[i].customString17 + toStr;

				var queryInsert = "INSERT INTO \"COS_DWH\".\"STG_EMP_JOB\" VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";
				st = conn.prepareStatement(queryInsert);

				//handle Strings
				//Change null values to a more friendly value for front-end
				if ( businessUnit === null || businessUnit === undefined || businessUnit === 'null' || businessUnit === 'undefined' ) {
				    businessUnit = 'Sin Asignar';
				}
				st.setString(1, businessUnit);
				st.setString(2, company);
				st.setString(3, costCenter);
				st.setString(4, standardJobCode);
				//Change null values to a more friendly value for front-end
				if ( subDepartment === null || subDepartment === undefined || subDepartment === 'null' || subDepartment === 'undefined' ) {
				    subDepartment = 'Sin Asignar';
				}
				st.setString(5, subDepartment);
								//Change null values to a more friendly value for front-end
				if ( department === null || department === undefined || department === 'null' || department === 'undefined' ) {
				    department = 'Sin Asignar';
				}
				st.setString(6, department);
				st.setString(7, emplStatus);
				st.setString(8, employeeType);
				st.setString(9, employmentType);
				st.setString(11, event);
				st.setString(12, eventReason);
				st.setString(13, holidayCalendarCode);
				st.setString(14, isFulltimeEmployee);
				st.setString(15, jobCode);
				st.setString(16, jobTitle);
				st.setString(17, location);
				st.setString(18, managerId);
				st.setString(19, position);
				st.setString(21, regularTemp);
				st.setString(22, retired);
				st.setString(25, userId);
				st.setString(26, workerCategory);
				st.setString(28, workscheduleCode);
				//Change null values to a more friendly value for front-end
				if ( division === null || division === undefined || division === 'null' || division === 'undefined' ) {
				    division = 'Sin Asignar';
				}
				st.setString(29, division);
				st.setString(31, personalDivision);

				//handle Dates
				if (endDate === null || endDate === undefined || endDate === 'null') {
					st.setDate(10, new Date(0));
				} else {
					endDate = new Date(eval(endDate.substring(6, endDate.length - 2)));
					st.setDate(10, endDate);
				}
				if (positionEntryDate === null || positionEntryDate === undefined || positionEntryDate === 'null') {
					st.setDate(20, new Date(0));
				} else {
					positionEntryDate = new Date(eval(positionEntryDate.substring(6, positionEntryDate.length - 2)));
					st.setDate(20, positionEntryDate);
				}
				if (startDate === null || startDate === undefined || startDate === 'null') {
					st.setDate(24, new Date(0));
				} else {
					startDate = new Date(eval(startDate.substring(6, startDate.length - 2)));
					st.setDate(24, startDate);
				}
				//handle Doubles / Integers
				if (standardHours === null || standardHours === undefined || standardHours === 'null' || standardHours === 'undefined' ) {
					st.setDouble(23, 0);
				} else {
					st.setDouble(23, parseFloat(standardHours));
				}
				if (workingDaysPerWeek === null || workingDaysPerWeek === undefined || workingDaysPerWeek === 'null' || workingDaysPerWeek === 'undefined' ) {
					st.setDouble(27, 0);
				} else {
					st.setDouble(27, parseFloat(workingDaysPerWeek));
				}
				if (occupationPercent === null || occupationPercent === undefined || occupationPercent === 'null' || occupationPercent === 'undefined' ) {
					st.setDouble(30, 0);
				} else {
					st.setDouble(30, parseFloat(occupationPercent));
				}
				st.execute();
				conn.commit();
			}
			conn.close();
		} catch (e) {
            console.log("responseinerror");
            //console.log(JSON.parse(e));
            //console.log($.response+"responsef");
            //console.log($.response.setBody);
			$.response.setBody("Audit " + occupationPercent + " " + workingDaysPerWeek + " " + standardHours );
			AuditError(auditId, 'EmpJob', e.message);
			return;
		}
	}
	//will iterate at least once for the case of no token provided
	while (iterationToken !== '')
	AuditFinalize(auditId);
}
//parseEmpJob();