
function sendUserEmail() {
    try {
      	var from = "joao.mycatmeows@gmail.com";
        //var to = $.request.parameters.get("joaop_m3@hotmail.com");
        var to = "joaop_m3@hotmail.com";
        //var subject = $.request.parameters.get("This is a test");
       // var message = $.request.parameters.get("Apparently gmail sends email if you ask nicely");
        var subject = "This is a test";
        var message = "Apparently gmail sends email if you ask nicely";
        
        var mail = new $.net.Mail({
        	    sender: {address: from},
        	    to: [{ address: to}],
        	    subject: "Subject : " + subject + " ",
        	    subjectEncoding:"UTF-8",
        	    parts: [ new $.net.Mail.Part({
        	        type: $.net.Mail.Part.TYPE_TEXT,
        	        contentType: "text/plain", 
        	        text: message,
        	        encoding:"UTF-8"
        	    })]
        });
        var returnValue = mail.send();
        var response = "MessageId = " + returnValue.messageId + ", final reply = " + returnValue.finalReply;
        
        $.response.setBody(JSON.stringify(response));
    }  
    catch (e) {  
        $.response.setBody(e.message + " ");
    }  
}

sendUserEmail();