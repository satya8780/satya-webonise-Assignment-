class directory_tree {
 
  file { [  '/var/www/test-app', '/var/www/test-app/current', '/var/www/test-app/current/index.html'
            '/var/www/test-app/releases', '/var/www/test-app/shared' ]:
    #ensure => 'directory',
  }

  file { $test-app:
    ensure => 'directory',
    owner   => 'root',
    group   => 'root',
    mode    => '0755'
  }
 }

# NGINX Configuration
  file { '/etc/nginx/ssl':
    ensure => directory,
    owner => 'root',
    group => 'root'
  }
  file { '/etc/nginx/ssl/example.com.crt':
    ensure => file,
    owner => 'root',
    group => 'root',
    mode => '0644',
    source => 'puppet:///modules/nginx/example.com.crt'
  }
  file { '/etc/nginx/ssl/example.com.key':
    ensure => file,
    owner => 'root',
    group => 'root',
    mode => '0644',
    source => 'puppet:///modules/nginx/example.com.key'
  }

$server_name = "testapi.example.com"
  nginx::resource::server {"$server_name":
    ssl                  => true,
    ssl_port             => 443,
    ssl_redirect         => true,
    ssl_cert             => "/etc/nginx/ssl/example.com.crt",
    ssl_key              => "/etc/nginx/ssl/example.com.key",
    ssl_protocols        => 'TLSv1.2 TLSv1.1 TLSv1',
    ensure               => present,
    use_default_location => false,
    www_root             => "/var/www/test-app/current/"
  }
  nginx::resource::location {"/":
    server                => "$server_name",
    ensure                => present,
    www_root             => "/var/www/test-app/current/",
    priority              => 401
  }

class line::conf {
  file { '/etc/nginx/nginx.conf':
  ensure => present,
}
file_line { 'Replacing a line to /etc/nginx/nginx.conf:
  path => '/etc/nginx/nginx.conf',
  replace => true,
  line => 'worker_processes  2',
  match   => "^worker_processes  1*$"
}
}
