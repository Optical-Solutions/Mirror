{
	"jobDefinitionName": "mvms-dev-altria",
	"type": "container",
	"parameters": {},
	"containerProperties": {
		"image": "${pullImageFromAccountId}.dkr.ecr.us-gov-west-1.amazonaws.com/mccs/developers/app/mvms-middleware:${containerImageTag}",
		"command": ["perl","/home/perl/bin/altria/altria.pl","--po","2757903"],
		"executionRoleArn": "${executionRoleArn}",
		"volumes": [
			{
				"name": "mvms-middleware-efs",
				"efsVolumeConfiguration": {
					"fileSystemId": "fs-0982ec6bdccc54046",
					"rootDirectory": "/mvms-middleware-tmpfs"
				}
			},
			{
				"name": "mvms-middleware-efs-var-log",
				"efsVolumeConfiguration": {
					"fileSystemId": "fs-0982ec6bdccc54046",
					"rootDirectory": "/mvms-middleware-var-log"
				}
			},
			{
				"name": "mvms-middleware-efs-var-run",
				"efsVolumeConfiguration": {
					"fileSystemId": "fs-0982ec6bdccc54046",
					"rootDirectory": "/mvms-middleware-var-run"
				}
			},
			{
				"name": "mvms-middleware-efs-var-lib-logrotate",
				"efsVolumeConfiguration": {
					"fileSystemId": "fs-0982ec6bdccc54046",
					"rootDirectory": "/mvms-middleware-var-lib-logrotate"
				}
			},
			{
				"name": "mvms-middleware-efs-usr-local-mccs-data",
				"efsVolumeConfiguration": {
					"fileSystemId": "fs-0982ec6bdccc54046",
					"rootDirectory": "/mvms-middleware-usr-local-mccs-data"
				}
			},
			{
				"name": "mvms-middleware-efs-usr-local-mccs-log",
				"efsVolumeConfiguration": {
					"fileSystemId": "fs-0982ec6bdccc54046",
					"rootDirectory": "/mvms-middleware-usr-local-mccs-log"
				}
			}
		],
		"environment": [
			{
				"name": "ENVIRONMENT",
				"value": "DEV"
			}
		],
		"mountPoints": [
			{
				"sourceVolume": "mvms-middleware-efs",
				"readOnly": false,
				"containerPath": "/secondary"
			},
			{
				"sourceVolume": "mvms-middleware-efs-var-log",
				"containerPath": "/var/log",
				"readOnly": false
			},
			{
				"sourceVolume": "mvms-middleware-efs-var-run",
				"containerPath": "/var/run",
				"readOnly": false
			},
			{
				"sourceVolume": "mvms-middleware-efs-var-lib-logrotate",
				"containerPath": "/var/lib/logrotate",
				"readOnly": false
			},
			{
				"sourceVolume": "mvms-middleware-efs-usr-local-mccs-data",
				"containerPath": "/usr/local/mccs/data",
				"readOnly": false
			},
			{
				"sourceVolume": "mvms-middleware-efs-usr-local-mccs-log",
				"containerPath": "/usr/local/mccs/log",
				"readOnly": false
			}
		],
		"ulimits": [],
		"resourceRequirements": [
			{
				"value": "1.0",
				"type": "VCPU"
			},
			{
				"value": "2048",
				"type": "MEMORY"
			}
		],
		"secrets": [
			{
				"name": "SPS-DLA",
				"valueFrom": "arn:aws-us-gov:secretsmanager:us-gov-west-1:142110233856:secret:MVMS-Middleware-SPS-DLA-EsYD1N"
			},
			{
				"name": "MVMS-Middleware-RdiUser",
				"valueFrom": "arn:aws-us-gov:secretsmanager:us-gov-west-1:142110233856:secret:MVMS-Middleware-RdiUser-XicrCL"
			}
		],
		"networkConfiguration": {
			"assignPublicIp": "DISABLED"
		},
		"fargatePlatformConfiguration": {
			"platformVersion": "LATEST"
		},
		"runtimePlatform": {
			"operatingSystemFamily": "LINUX",
			"cpuArchitecture": "X86_64"
		}
	},
	"tags": {},
	"platformCapabilities": [
		"FARGATE"
	]
}