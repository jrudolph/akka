import akka._

AkkaBuild.defaultSettings
Formatting.formatSettings
OSGi.stream
Dependencies.stream

enablePlugins(spray.boilerplate.BoilerplatePlugin)

libraryDependencies += "org.hdrhistogram"            % "HdrHistogram"                 % "2.1.9"