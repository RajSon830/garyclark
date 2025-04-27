<?php 

namespace App\Controller;

use Raj\Framework\Http\RedirectResponse;
use Raj\Framework\Http\Request;
use Raj\Framework\Http\Response;
use App\Entity\Post;
use App\Repository\PostMapper;
use App\Repository\PostRepository;
use Raj\Framework\Session\SessionInterface;

class PostController extends AbstractCotroller{

    public function __construct(private PostMapper $postMapper,private PostRepository $postRepository,private SessionInterface $session){

    }


    public function show(int $id):Response{

        $post = $this->postRepository->findOrFail($id);

        if(is_null($post)){

        }

        return $this->render('posts.html.twig',[
            'post'=>$post
        ]);
    }

    public function create(){
        return $this->render('create-post.html.twig');
    }


    public function store(): Response{

        $title = $this->request->getPostParams('title');
        $body = $this->request->getPostParams('body');

        $post = Post::create($title,$body);
        /**
         * We need Post Mapper class to save the POST Entity into the database directly
         * $this->postMapper->save($post);
         */

        $this->postMapper->save($post);
        # dd($post);
        $this->session->setFlash('success',sprintf('Post "%s" successfully created',$title));

        return new RedirectResponse('/post');
    }


}