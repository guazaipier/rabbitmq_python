@REM 生成密码
echo -n "monitoring" | openssl md5
@REM 添加用户
rabbitmqctl add_user 'monitoring' '0642ad26a822c6b77c76d8cf7dc7d336'
@REM 设置标签
rabbitmqctl.bat set_user_tags 'monitoring' 'monitoring' 
@REM 设置权限
rabbitmqctl.bat set_permissions --vhost '/' 'monitoring' '^$' '^$' '^$'
@REM rabbitmqctl.bat list_vhosts
@REM rabbitmqctl.bat list_users
@REM rabbitmqctl.bat list_permissions

@REM 网页端可视化管理工具 RabbitMQ Management
@REM 通过 localhost:15672 登录 RabbitMQ 管理界面，用户名密码为 monitoring/0642ad26a822c6b77c76d8cf7dc7d336
@REM 或通过 主机名 DESKTOP-21KSDQL.local:15672 登录 RabbitMQ 管理界面，用户名密码为 monitoring/0642ad26a822c6b77c76d8cf7dc7d336

@REM 官网直达：https://rabbitmq.org.cn/docs/management#single-listener-port