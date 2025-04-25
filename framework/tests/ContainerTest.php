<?php 

namespace Raj\Framework\Tests;


use PHPUnit\Framework\TestCase;
use Raj\Framework\Container\Container;
use Raj\Framework\Container\ContainerException;
use Raj\Framework\Tests\DependencyClass;
use Raj\Framework\Tests\DependentClass;


class ContainerTest extends TestCase{

    /** @test */ 
    public function testServiceCanBeRetrievedFromTheContainer(){

        // Setup 
        $container = new Container();

        // Do Something 
        // id string, concrete class name string or object
        $container->add('dependent-class',DependentClass::class);


        // make assertion 
        $this->assertInstanceOf(DependentClass::class,$container->get('dependent-class'));

    }

    /** @test */ 
    public function testExceptionIsThrownIfServiceNotFound(){

        $container = new Container();

        // Excpert exception
        $this->expectException(ContainerException::class);

        // Do Something
        $container->add('foobar');


    }

    public function testContainerHasService(){

        //setup 
        $container = new Container();

        $container->add('dependent-class',DependentClass::class);

        $this->assertTrue($container->has('dependent-class'));

        $this->assertFalse($container->has('non-exist'));

    }

    public function testServiceCanBeRecursivelyAutowired(){

        $container = new Container();

        $container->add('dependent-service',DependentClass::class);

        $dependentService = $container->get('dependent-service');

        $depencyService = $dependentService->getDependency();

        $this->assertInstanceOf(DependencyClass::class,$dependentService->getDependency());

        $this->assertInstanceOf(SubDependencyClass::class,$depencyService->getSubDependency());

    }



}
