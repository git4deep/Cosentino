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

function parseJobRequisitionStatus() {
    try{
        var auditId = AuditInitialize('Job Application Audit');
    	var dest = $.net.http.readDestination("SSFF");
    	var response;
    	var content;
    	var client = new $.net.http.Client();
    	var toStr = "";
    	var i = 0;
    	var conn = $.db.getConnection();

    	var jobRequisitionIds = [];
    	var querySelect =
    	    "SELECT DISTINCT \"jobReqId\" FROM \"COS_DWH\".\"STG_JOB_REQUISITION\"";

    	//	"SELECT DISTINCT \"jobReqId\" FROM \"COS_DWH\".\"STG_JOB_REQUISITION\" WHERE \"jobReqId\" NOT IN ( SELECT DISTINCT \"jobReqId\" FROM \"COS_DWH\".\"COS_DWH\".\"STG_JOB_REQUISITION_CANCELLATIONS\")";
        var st = conn.prepareStatement(querySelect);
    	var rs = st.executeQuery();
    	while (rs.next()) {
    		if (rs.getInteger(1) !== undefined) {
    			jobRequisitionIds[i] = rs.getInteger(1);
    			i++;
    		}
    	}
    	conn.close();
    	conn = $.db.getConnection();
    	client = new $.net.http.Client();
        var reqID;

    }catch(e){
        //AuditError(auditId, 'Job Application Audit', reqID + " " + e.message);
        $.response.setBody('1 - Finished Job Requisition: ' + jobRequisitionIds[i]);
		return;
    }
	for (i = 0; i < jobRequisitionIds.length; i++) {

		try {
			reqID = jobRequisitionIds[i];
			var str = '/Requisition(' + reqID +')/status?$format=json';
			var TokenReq = new $.web.WebRequest($.net.http.GET, str);
			TokenReq.headers.set('Authorization', 'Basic QURNSU5DT1NFTlRJTk9AQzAwMDM1NTgwNTBQOkNvc2VudGlubzE3');
			client.request(TokenReq, dest);
			response = client.getResponse();
			var parseAttempt;
			try{
			    content = JSON.parse(response.body.asString());
			    parseAttempt = content.d.results;
			}
			catch(e){
			    if(parseAttempt === undefined){
			        continue;
			    }
			    $.response.setBody('Finished Job Requisition: ' + reqID + ' ' + parseAttempt);
			    return;
			}
			//look for externalCode = 'reqStatus_Cancelled' -> update STG_JOB_REQ to 'Deleted' if so
			
			//0 lenght applications have the applicationID informed, but nothing else. These need to be loaded with 'null' data to avoid endless loops;
			var totalParses;
			if(parseAttempt.length === 0){
			    totalParses = 1;
			}
			else{
			    totalParses = parseAttempt.length;
			}
			for (var parseIdx = 0; parseIdx < totalParses; parseIdx++) {
                //initialize dummy variables in case of started application with no data
                var anExternalCode = "null";
                
                
                if( parseAttempt[parseIdx] ){
                    anExternalCode = parseAttempt[parseIdx].externalCode + toStr;
                }
                
                if( anExternalCode !== 'reqStatus_Cancelled'){
                    continue;    
                }
                var queryInsert = 'INSERT INTO \"COS_DWH\".\"STG_JOB_REQUISITION_CANCELLATIONS\" VALUES (?,1)';
				st = conn.prepareStatement(queryInsert);
                st.setString(1, reqID);
                
				st.executeUpdate();
				conn.commit();
			} 
			$.response.setBody('Finished Job Requisition: ' + jobRequisitionIds[i]);
		} catch (e) {
			//AuditError(auditId, 'Job Application Audit', reqID + " " + e.message);
			$.response.setBody('Finished Job Requisition: ' + reqID + " " + e.message);
			return;
		}
	}
	conn.close();
	client.close();
	AuditFinalize(auditId);
}

parseJobRequisitionStatus();