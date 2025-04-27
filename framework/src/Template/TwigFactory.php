<?php 

namespace Raj\Framework\Template;

use Raj\Framework\Session\SessionInterface;
use Twig\Environment;
use Twig\Extension\DebugExtension;
use Twig\Loader\FilesystemLoader;
use Twig\TwigFunction;

class TwigFactory{

    public function __construct(private SessionInterface $session,private string $tempaltePath){

    }

    public function create():Environment{

        // instantiate FileSysteLoader with template path 
        $loader = new FilesystemLoader($this->tempaltePath);

        // instantiate Twig Enviroment with loader
        $twig = new Environment($loader, [
            'debug'=>true,
            'cache'=>false
        ]);

        // add new twig session() function to Environment
        $twig->addExtension(new DebugExtension());
        $twig->addFunction(new TwigFunction('session',[$this,'getSession']));

        return $twig;

    }

    public function getSession():SessionInterface
    {
        return $this->session;
    }
}