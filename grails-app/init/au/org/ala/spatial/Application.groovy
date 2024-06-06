package au.org.ala.spatial

import grails.boot.GrailsApp
import grails.boot.config.GrailsAutoConfiguration
import groovy.transform.CompileStatic
import org.grails.io.support.PathMatchingResourcePatternResolver
import org.grails.io.support.Resource
import org.springframework.context.annotation.ComponentScan

@CompileStatic
@ComponentScan
class Application extends GrailsAutoConfiguration {
    static void main(String[] args) {

        def resolver = new PathMatchingResourcePatternResolver()
        Resource[] resources = resolver.getResources("/processes/*.json") ;
        for (Resource resource: resources) {
            System.out.println(resource.getFilename())
        }
        GrailsApp.run(Application, args)
    }
}
