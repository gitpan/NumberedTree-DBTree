use 5.008;
use ExtUtils::MakeMaker;

WriteMakefile(
    'NAME'		=> 'NumberedTree::DBTree',
    'VERSION_FROM'	=> 'DBTree.pm', 
    'PREREQ_PM'		=> {NumberedTree => 1.0}, 
    ($] >= 5.005 ?   
      (AUTHOR     => 'Yosef Meller <mellerf@netvision.net.il>') : ()),
);
