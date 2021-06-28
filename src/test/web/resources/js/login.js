
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
		url: 'http://localhost:8080/login',
		type: 'POST',
		dataType : 'json',
		contentType : 'application/json',
		data: JSON.stringify({
			userName: $('#username').val(),
			passWord: $('#password').val(),
			//rememberMe: $('#rememberMe').is(':checked'),
			backurl: 'index.html'
		}),

		beforeSend: function() {

		},
		success: function(json){

			window.localStorage.setItem("user_name", json.user_name);
			window.localStorage.setItem("user_name_1", json.user_name);
			window.localStorage.setItem("user_id", json.id);
			window.localStorage.setItem("user_e_mail", json.e_mail);
			window.localStorage.setItem("user_birthday", json.birthday);
			window.localStorage.setItem("user_mobile", json.mobile);
			window.localStorage.setItem("user_money", json.money);
			window.localStorage.setItem("user_money_pwd", json.money_pwd);
			window.localStorage.setItem("user_last_login_time", json.last_login_time);
			window.localStorage.setItem("user_register_time", json.register_time);
			window.localStorage.setItem("user_qq", json.qq);
			window.localStorage.setItem("user_job", json.job);
			window.localStorage.setItem("user_political_face", json.political_face);
			window.localStorage.setItem("user_age_class", json.age_class);
			window.localStorage.setItem("user_nationality", json.nationality);
			window.localStorage.setItem("user_marriage", json.marriage);
			window.localStorage.setItem("user_is_in_blacklist", json.is_in_blacklist);
			window.localStorage.setItem("user_constellation", json.constellation);
			window.localStorage.setItem("user_payment_way", json.payment_way);
			window.localStorage.setItem("user_ave_price", json.ave_price);
			window.localStorage.setItem("user_ave_price_range", json.ave_price_range);
			window.localStorage.setItem("user_order_count", json.order_count);
			window.localStorage.setItem("user_frequency", json.frequency);
			window.localStorage.setItem("user_register_time", json.register_time);
			window.localStorage.setItem("user_good_bought", json.good_bought);

			window.localStorage.setItem("user_ability", json.消费能力);
			window.localStorage.setItem("user_discount", json.消费优惠券依赖度);
			// window.localStorage.setItem("user_name", json.user_name);
			// window.localStorage.setItem("user_name", json.user_name);
			// window.localStorage.setItem("user_name", json.user_name);
			// window.localStorage.setItem("user_name", json.user_name);




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
			alert("账户或者密码错误，请重新输入");
			$('#password').focus();
		}
	});


	$.ajax({
		url: 'http://localhost:8080/read_top',
		type: 'POST',
		dataType : 'json',
		contentType : 'application/json',
		data: JSON.stringify({
			id: window.localStorage.getItem("user_id"),
			// passWord: $('#password').val(),
			// rememberMe: $('#rememberMe').is(':checked'),
			// backurl: 'index.html'
		}),

		beforeSend: function() {

		},
		success: function(json){
			console.log(json.top1);
			// console.log(window.localStorage.getItem("user_name"));
			//
			// Tab.closeTab($('#tab_profile_1_html'));
			//
			// Tab.addTab('个人资料', 'profile_1.html');


			window.localStorage.setItem("user_top_1", json[0].top1);
			window.localStorage.setItem("user_top_2", json[0].top2);
			// window.localStorage.setItem("user_name", json.user_name);
			// window.localStorage.setItem("user_name", json.user_name);
			// window.localStorage.setItem("user_name", json.user_name);
			// window.localStorage.setItem("user_name", json.user_name);


			//location.href = "index.html";

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
			alert("号码输入错误请重新输入");
			$('#keywords').focus();
		}
	});


}



