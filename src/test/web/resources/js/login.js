$(function() {
	// Waves初始化
	Waves.displayEffect();
	// 输入框获取焦点后出现下划线
	$('.form-control').focus(function() {
		$(this).parent().addClass('fg-toggled');
	}).blur(function() {
		$(this).parent().removeClass('fg-toggled');
	});
});
Checkbix.init();
$(function() {
	// 点击登录按钮
	$('#login-bt').click(function() {
		login();
	});
	// 回车事件
	$('#username, #password').keypress(function (event) {
		if (13 === event.keyCode) {
			login();
		}
	});
});
// 登录
function login() {
	$.ajax({
		url: 'http://192.168.101.199:8080/login',
		type: 'POST',
		dataType : 'json',
		contentType : 'application/json',
		data: JSON.stringify({
			userName: $('#username').val(),
			passWord: $('#password').val(),
			rememberMe: $('#rememberMe').is(':checked'),
			backurl: 'index.html'
		}),

		beforeSend: function() {

		},
		success: function(json){
			console.log(json.id)
			location.href = "index.html";

			//
			// if (json.code === 1) {
			// 	location.href = "index.html";
			// } else {
			// 	alert(json.data);
			// 	if (10101 === json.code) {
			// 		$('#username').focus();
			// 	}
			// 	if (10102 === json.code) {
			// 		$('#password').focus();
			// 	}
			// }
		},
		error: function(error){
			alert("账户或者错误，请重新输入")
		}
	});
}