<?php 

namespace Framework\Tests;

use PHPUnit\Framework\TestCase;
use Raj\Framework\Container\Container;
use Raj\Framework\Container\ContainerException;
use Raj\Framework\Container\DependantClass;

class ContainerTest extends TestCase{

    /** @test */ 
    public function testServiceCanBeRetrievedFromTheContainer(){

        // Setup 
        $container = new Container();

        // Do Something 
        // id string, concrete class name string or object
        $container->add('dependant-class',DependantClass::class);


        // make assertion 
        $this->assertInstanceOf(DependantClass::class,$container->get('dependant-class'));

    }

    /** @test */ 
    public function testExceptionIsThrownIfServiceNotFound(){

        $container = new Container();

        // Excpert exception
        $this->expectException(ContainerException::class);

        // Do Something
        $container->add('foobar');


    }


}
