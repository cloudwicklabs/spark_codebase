# -*- mode: ruby -*-
# vi: set ft=ruby :

$script = <<SCRIPT
cat << EOF >> /etc/hosts
192.168.34.100 sparkmaster
192.168.34.101 sparkworker1
192.168.34.102 sparkworker2
EOF

sudo service iptables stop
sudo chkconfig iptables stop
sudo /usr/sbin/setenforce 0
sudo sed -i.old s/SELINUX=enforcing/SELINUX=disabled/ /etc/selinux/config
SCRIPT

boxes = [
    { :name => :sparkmaster,  :ip => '192.168.34.100', :cpus => 2, :memory => 1024 },
    { :name => :sparkworker1, :ip => '192.168.34.101', :cpus => 2, :memory => 2048 },
    { :name => :sparkworker2, :ip => '192.168.34.102', :cpus => 2, :memory => 2048 },
]

VAGRANT_API_VERSION = "2"

Vagrant.configure(VAGRANT_API_VERSION) do |conf|
  conf.vm.box = "chef/centos-6.5"

  boxes.each do |box|
    conf.vm.define box[:name] do |config|
      config.vm.network 'private_network', ip: box[:ip]
      config.vm.hostname = box[:name].to_s
      config.vm.provider "virtualbox" do |v|
        v.customize ["modifyvm", :id, "--memory", box[:memory]]
        v.customize ["modifyvm", :id, "--cpus", box[:cpus]]
      end

      #provisioning
      config.vm.provision :shell, inline: $script
    end
  end
end
