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

function fetchToken() {
	let client = new $.net.http.Client();
	let dest = $.net.http.readDestination("Intranet");
	let TokenReq = new $.web.WebRequest($.net.http.GET, '/intranet/api/basicauth/token');
	client.request(TokenReq, dest);
	let token = client.getResponse().body.asString();
	return token;
}
function parseIntranetKPI22(apiTypeId, date)
{
    var audit_id = AuditInitialize('IntranetKPI ' || apiTypeId);
	var conn = $.db.getConnection();
	var st;
	var dest = $.net.http.readDestination("IntranetKPI");
	var response;
	var content;
	var parseAttempt;
	var client = new $.net.http.Client();
	
		try {
		var TokenReq = new $.web.WebRequest($.net.http.GET, '/intranet/onboarding/api/kpi22?page=0&size=5000');
		TokenReq.headers.set('Authorization', 'Bearer ' + fetchToken());
		client.request(TokenReq, dest);
		response = client.getResponse().body.asString();
		//If no new data is received, do nothing
		if (response === undefined || response.length === 2) {
			return;
		}
		//New data received, clear Staging table
		var queryTruncate = 'TRUNCATE TABLE \"COS_DWH\".\"STG_INTRANET_KPI_TYPE_16\"';
		st = conn.prepareStatement(queryTruncate);
		st.executeUpdate();
		//Parse new data so it can be loaded into the table
		content = JSON.parse(response);
		parseAttempt = content;
	} catch (e) {
		AuditError(audit_id, 'IntranetKPI ' || apiTypeId, e.message);
	} finally {
		client.close();
	}
	//start loading the data to the tables
	var toStr = "";
	var connectionTreshold = 500;
	 
	 
	 var tmp = parseAttempt[0] + toStr;
	 var stringEndIdx = tmp.indexOf(',');
	 var tmp2 = tmp.substring(0, tmp.length);
	 //var tmp2 = tmp.replace(/['"]+/g, '');
	 $.response.setBody("Temp - " + tmp2.result);
	// $.response.setBody("parseAttempt - " + parseAttempt[2].result + toStr);
	 
/*	try {
		for (var i = 0; i < parseAttempt.length; i++) {
			var date = null;
			
			var value = parseAttempt[i].result + toStr;
			var employeeId = parseAttempt[i].employeeID + toStr;
			var typeId = '16';
			//Prepare Insert Statement
			
            $.response.setBody("parseAttempt - " + parseAttempt[i] + toStr);
			var queryInsert = "	INSERT INTO \"COS_DWH\".\"STG_INTRANET_KPI_TYPE_16\" VALUES (?,?,?,?)";
			st = conn.prepareStatement(queryInsert);
			//handle Dates
			if (date === null || date === undefined || date.trim() === 'null') {
				st.setDate(1, new Date(0));
			} else {
				st.setDate(1, date);
			}
			//handle Strings
			var stringStartIdx = value.indexOf(':');
			var stringEndIdx = value.indexOf(',');
			var valueTrimmed = value.substring(stringStartIdx + 1, stringEndIdx).replace(/['"]+/g, '');

			st.setString(2, valueTrimmed);
			//handle Integers
			if (employeeId === null || employeeId === undefined || employeeId.trim() === 'null') {
				st.setInt(3, 0);
			} else {
				st.setInt(3, parseFloat(employeeId));
			}
			if (typeId === null || typeId === undefined || typeId.trim() === 'null') {
			//	st.setInt(4, 0);
			} else {
			//	st.setInt(4, parseFloat(typeId));
			}
			//st.execute();
			conn.commit();
		}
	} catch (e) {
		AuditError(audit_id, 'IntranetKPI ' || apiTypeId, e.message);
	}
	conn.close();
	
	*/
	AuditFinalize(audit_id);
	
}

function parseIntranet(apiTypeId, today, table) {
	var audit_id = AuditInitialize('IntranetKPI ' || apiTypeId);
	var conn = $.db.getConnection();
	var st;
	var dest = $.net.http.readDestination("IntranetKPI");
	var response;
	var content;
	var parseAttempt;
	var client = new $.net.http.Client();
	try {
		var TokenReq = new $.web.WebRequest($.net.http.GET, '/intranet/successfactor/api/kpis?typeId.equals=' + apiTypeId + '&date.equals=' + today);
		TokenReq.headers.set('Authorization', 'Bearer ' + fetchToken());
		client.request(TokenReq, dest);
		response = client.getResponse().body.asString();
		//If no new data is received, do nothing
		if (response === undefined || response.length === 2) {
			return;
		}
		//New data received, clear Staging table
		var queryTruncate = 'TRUNCATE TABLE \"COS_DWH\".\"STG_INTRANET_KPI' + table + '\"';
		st = conn.prepareStatement(queryTruncate);
		st.executeUpdate();
		//Parse new data so it can be loaded into the table
		content = JSON.parse(response);
		parseAttempt = content;
	} catch (e) {
		AuditError(audit_id, 'IntranetKPI ' || apiTypeId, e.message);
	} finally {
		client.close();
	}
	//start loading the data to the tables
	var toStr = "";
	var connectionTreshold = 500;
	try {
		for (var i = 0; i < parseAttempt.length; i++) {
			var date = parseAttempt[i].date + toStr;
			var value = parseAttempt[i].result + toStr;
			var employeeId = parseAttempt[i].employeeId + toStr;
			var typeId = parseAttempt[i].typeId + toStr;
			//Prepare Insert Statement
			var queryInsert = "	INSERT INTO\"COS_DWH\".\"STG_INTRANET_KPI" + table + "\" VALUES (?,?,?,?)";
			st = conn.prepareStatement(queryInsert);
			//handle Dates
			if (date === null || date === undefined || date.trim() === 'null') {
				st.setDate(1, new Date(0));
			} else {
				st.setDate(1, date);
			}
			//handle Strings
			var stringStartIdx = value.indexOf(':');
			var stringEndIdx = value.indexOf(',');
			var valueTrimmed = value.substring(stringStartIdx + 1, stringEndIdx).replace(/['"]+/g, '');

			st.setString(2, valueTrimmed);

			//handle Integers
			if (employeeId === null || employeeId === undefined || employeeId.trim() === 'null') {
				st.setInt(3, 0);
			} else {
				st.setInt(3, parseFloat(employeeId));
			}
			if (typeId === null || typeId === undefined || typeId.trim() === 'null') {
				st.setInt(4, 0);
			} else {
				st.setInt(4, parseFloat(typeId));
			}
			st.execute();
			conn.commit();
		}
	} catch (e) {
		AuditError(audit_id, 'IntranetKPI ' || apiTypeId, e.message);
		return;
	}
	conn.close();
	AuditFinalize(audit_id);
}

function parseIntranetPaoStatusType(apiTypeId, today, table) {
	var audit_id = AuditInitialize('IntranetKPI ' || apiTypeId);
	var conn = $.db.getConnection();
	var st;
	var dest = $.net.http.readDestination("IntranetKPI");
	var response;
	var content;
	var parseAttempt;
	var client = new $.net.http.Client();
	try {
		//fetch new Bearer Token
		var TokenReq = new $.web.WebRequest($.net.http.GET, '/intranet/successfactor/api/kpis?typeId.equals=' + apiTypeId + '&date.equals=' + today);
		TokenReq.headers.set('Authorization', 'Bearer ' + fetchToken());
		client.request(TokenReq, dest);
		response = client.getResponse().body.asString();
		//If no new data is received, do nothing
		
		$.response.setBody(response);
		//return;
		
		
		if (response === undefined || response.length === 2) {
			return;
		}
		//New data received, clear Staging table
		var queryTruncate = 'TRUNCATE TABLE \"COS_DWH\".\"STG_INTRANET_KPI' + table + '\"';
		st = conn.prepareStatement(queryTruncate);
		st.executeUpdate();
		//Parse new data so it can be loaded into the table
		content = JSON.parse(response);
		parseAttempt = content;
	} catch (e) {
		AuditError(audit_id, 'IntranetKPI ' || apiTypeId, e.message);
	} finally {
		client.close();
	}
	//start loading the data to the tables
	var toStr = "";
	try {
		for (var i = 0; i < parseAttempt.length; i++) {
			//common elements
			var date = parseAttempt[i].date + toStr;
			var value = parseAttempt[i].result + toStr;
			var employeeId = parseAttempt[i].employeeId + toStr;
			var typeId = parseAttempt[i].typeId + toStr;
			 $.response.setBody("parseAttempt - " + parseAttempt[i].result + toStr);
			//Prepare Insert Statement
			var queryInsert;
			if (apiTypeId === 15 || apiTypeId === 12) {
				queryInsert = "	INSERT INTO\"COS_DWH\".\"STG_INTRANET_KPI" + table + "\" VALUES (?,?,?,?,?,?,?,?,?,?,?)";
			}
			if (apiTypeId === 17) {
				queryInsert = "	INSERT INTO\"COS_DWH\".\"STG_INTRANET_KPI" + table + "\" VALUES (?,?,?,?)";
			}
			st = conn.prepareStatement(queryInsert);

			//handle Dates
			if (date === null || date === undefined || date === 'null') {
				st.setDate(1, new Date(0));
			} else {
				st.setDate(1, date);
			}
			//handle Strings
			var remainingText = value;
			var remainingTextStartIdx;
			var remainingTextEndIdx;
			var ohrp = '0';

			if (apiTypeId === 12) {
				//order is reversed for this KPI, employeeId comes prior to ohrp
				remainingTextStartIdx = remainingText.indexOf(':');
				remainingTextEndIdx = remainingText.indexOf(',');
				// var employeeId = remainingText.substring(remainingTextStartIdx, employeeIdEndIdx);
				remainingText = remainingText.substring(remainingTextEndIdx + 2);
				remainingTextStartIdx = remainingText.indexOf(':');
				remainingTextEndIdx = remainingText.indexOf(',');
				ohrp = remainingText.substring(remainingTextStartIdx + 1, remainingTextEndIdx);

			} else {
			    
			    remainingTextStartIdx = remainingText.indexOf(':');
				remainingTextEndIdx = remainingText.indexOf(',');
			    //ohrp may or may not exist
			    if (remainingText.contains('ohrp')){
				    ohrp = remainingText.substring(remainingTextStartIdx + 1, remainingTextEndIdx);
				    remainingText = remainingText.substring(remainingTextEndIdx + 2);
				    remainingTextStartIdx = remainingText.indexOf(':');
				    remainingTextEndIdx = remainingText.indexOf(',');
			    }
				//Employee Id is read from base level, so we skip this one
				// var employeeId = value.substring(remainingTextStartIdx, employeeIdEndIdx);
			}
			//only applies to some KPI
			var objectives;
			var status;
			if (apiTypeId === 15 || apiTypeId === 12) {
				remainingText = remainingText.substring(remainingTextEndIdx + 2);
				remainingTextStartIdx = remainingText.indexOf(':');
				remainingTextEndIdx = remainingText.indexOf(',');
				objectives = remainingText.substring(remainingTextStartIdx + 1, remainingTextEndIdx);
				//value for status is an array
				remainingText = remainingText.substring(remainingTextEndIdx + 2);
				remainingTextStartIdx = remainingText.indexOf('{');
				remainingTextEndIdx = remainingText.indexOf('}');
				remainingText = remainingText.substring(remainingTextStartIdx + 1, remainingTextEndIdx);
				status = remainingText;
				//status can have a variable number of values, including empty
				var columnText = 0;
				var columnValue = 0;
				var notStarted = 0;
				var inProgress = 0;
				var done = 0;
				var noStatus = 0;
				var cancelled = 0;
				while (remainingText.indexOf('"') > -1) {
					if (status === null || status === undefined || status === 'null' || status === 'status":null' || status.indexOf("null") > -1 || status.indexOf('{}}') > -1 ) {
						//do nothing
						break;
					} else {
						//we want the value inside the quotes
						remainingTextStartIdx = remainingText.indexOf('"');
						remainingTextEndIdx = remainingText.indexOf(':') - 1;
						columnText = remainingText.substring(remainingTextStartIdx + 1, remainingTextEndIdx);
						//we want a number value after ":" and before ","
						remainingText = remainingText.substring(remainingTextEndIdx);
						remainingTextStartIdx = 2;
						remainingTextEndIdx = 3;
						columnValue = remainingText.substring(remainingTextStartIdx, remainingTextEndIdx);
						//infer column to insert from the json text
						if (columnText === 'En Proceso') {
							inProgress = columnValue;
						}
						if (columnText === 'No iniciada') {
							notStarted = columnValue;
						}
						if (columnText === 'Finalizada') {
							done = columnValue;
						}
						if (columnText === 'Sin estado') {
							noStatus = columnValue;
						}
						if (columnText === 'Cancelada') {
							cancelled = columnValue;
						}
						remainingText = remainingText.substring(remainingTextEndIdx + 1);
					}
				}
				//handle KPI specific values
				if (!objectives || objectives === undefined || objectives.trim() === 'null') {
					st.setDouble(5, 0);
				} else {
					st.setDouble(5, parseFloat(objectives));
				}

				st.setString(6, status);

				if (!notStarted || notStarted === undefined || notStarted === 'null') {
					st.setInt(7, 0);
				} else {
					st.setInt(7, parseFloat(notStarted));
				}

				if (!inProgress || inProgress === undefined || inProgress.trim() === 'null') {
					st.setInt(8, 0);
				} else {
					st.setInt(8, parseFloat(inProgress));
				}

				if (!done || done === undefined || done.trim() === 'null') {
					st.setInt(9, 0);
				} else {
					st.setInt(9, parseFloat(done));
				}

				if (!noStatus || noStatus === undefined || noStatus.trim() === 'null') {
					st.setInt(10, 0);
				} else {
					st.setInt(10, parseFloat(noStatus));
				}

				if (!cancelled || cancelled === undefined || cancelled.trim() === 'null') {
					st.setInt(11, 0);
				} else {
					st.setInt(11, parseFloat(cancelled));
				}

			}

			//handle Integers
			if (ohrp === null || ohrp === undefined || ohrp.trim() === 'null') {
				st.setInt(2, 0);
			} else {
				st.setInt(2, parseFloat(ohrp));
			}

			if (!employeeId || employeeId === undefined || employeeId.trim() === 'null') {
				st.setInt(3, 0);
			} else {
				st.setInt(3, parseFloat(employeeId));
			}

			if (!typeId || typeId === undefined || typeId.trim() === 'null') {
				st.setInt(4, 0);
			} else {
				st.setInt(4, parseFloat(typeId));
			}

			st.execute();
			conn.commit();
		}
	} catch (e) {
		AuditError(audit_id, 'IntranetKPI ' || apiTypeId, e.message);
		return;
	}
	conn.close();
	AuditFinalize(audit_id);
}

function parseIntranetKPI() {
	var audit_id = AuditInitialize('IntranetKPI');
	//Obtain YYYY-MM-DD
	var today = new Date();
	var dd = today.getDate();
	var mm = today.getMonth() + 1; //January is 0!
	var yyyy = today.getFullYear();
	if (dd < 10) {
		dd = '0' + dd;
	}
	if (mm < 10) {
		mm = '0' + mm;
	}
	today = yyyy + '-' + mm + '-' + dd;
	//USE TO LOAD DATA OF A GIVEN DATE
    //today = '2020-02-28';
	//
	//call the api endpoint with this date
	//INITIAL LOAD --> today = '2019-03-14';
    parseIntranet(10, today, '_TYPE_10');
    parseIntranet(11, today, '_TYPE_11');
    parseIntranetPaoStatusType(12, today, '_TYPE_12');
    parseIntranet(14, today, '_TYPE_14');
    parseIntranetPaoStatusType(15, today, '_TYPE_15');
//	parseIntranetKPI22(16, today);
	parseIntranet(16, today, '_TYPE_16'); //// changed: 20191105 
 	parseIntranetPaoStatusType(17, today, '_TYPE_17');
	AuditFinalize(audit_id);
}

//parseIntranetKPI(); 