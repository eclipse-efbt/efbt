pipeline {
    agent any
    tools {
        maven 'apache-maven-latest'
        jdk 'adoptopenjdk-openj9-jdk11-latest'
    }
    stages {
        stage('Build') {
            steps {
                sh '''                                     
                    cd org.eclipse.efbt.cocamo.core
                    mvn install
                    cd ..
                    cd org.eclipse.efbt.dependencies.accessdb
                    mvn install
                    cd ..
                    cd org.eclipse.efbt.cocamo.smcubes
                    mvn install
                    cd ..
                     cd org.eclipse.efbt.cocamo.functions
                    mvn install
                    cd ..
                     cd org.eclipse.efbt.cocamo.lineage_core
                    mvn install
                    cd ..
                    cd org.eclipse.efbt.cocamo.attributelineage
                    mvn install
                    cd ..
                    cd org.eclipse.efbt.cocamo.datalineage
                    mvn install
                    cd ..                    
                    cd org.eclipse.efbt.ldm
                    mvn install
                    cd ..
                    cd org.eclipse.efbt.controller.model
                    mvn install
                    cd ..
                    cd org.eclipse.efbt.cocason.dsl.cocason.parent -DskipTests
                    mvn install
                    cd ..                    
                    cd org.eclipse.efbt.cocason
                    mvn install 
                    cd ..
                    cd org.eclipse.efbt.controller.component
                    mvn install
                    cd ..
                    cd org.eclipse.efbt.complete_repo
                    mvn install
                    cd ..
                '''
            }
        }
        stage('Deploy') {
	      steps {
	        sshagent ( ['projects-storage.eclipse.org-bot-ssh']) {
	          sh '''
	            ssh genie.efbt@projects-storage.eclipse.org rm -rf /home/data/httpd/download.eclipse.org/efbt/snapshots
	            ssh genie.efbt@projects-storage.eclipse.org mkdir -p /home/data/httpd/download.eclipse.org/efbt/snapshots	            	            	         
	            scp -r org.eclipse.efbt.complete_repo/releng/org.eclipse.efbt.repository/target/repository/* genie.efbt@projects-storage.eclipse.org:/home/data/httpd/download.eclipse.org/efbt/snapshots
	           '''
	        }
	      }
    }
    }
    post {
        // send a mail on unsuccessful and fixed builds
        unsuccessful { // means unstable || failure || aborted
            emailext subject: 'Build $BUILD_STATUS $PROJECT_NAME #$BUILD_NUMBER!', 
            body: '''Check console output at $BUILD_URL to view the results.''',
            recipientProviders: [culprits(), requestor()], 
            to: 'other.recipient@domain.org'
        }
        fixed { // back to normal
            emailext subject: 'Build $BUILD_STATUS $PROJECT_NAME #$BUILD_NUMBER!', 
            body: '''Check console output at $BUILD_URL to view the results.''',
            recipientProviders: [culprits(), requestor()], 
            to: 'other.recipient@domain.org'
        }
    }
}