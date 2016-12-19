import akka.{AkkaBuild, Dependencies, Formatting, OSGi}

AkkaBuild.defaultSettings
Formatting.formatSettings
OSGi.remote
Dependencies.remote

parallelExecution in Test := false

libraryDependencies += "org.hdrhistogram"            % "HdrHistogram"                 % "2.1.9"