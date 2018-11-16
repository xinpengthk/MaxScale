#include "nodes.h"
#include <string>
#include <cstring>
#include <iostream>
#include <vector>

using std::cout;
using std::endl;

std::vector<Nodes*> all_nodes;

Nodes::Nodes()
{
    all_nodes.push_back(this);
}

int Nodes::check_node_ssh(int node)
{
    int res = 0;

    if (ssh_node(node, (char*) "ls > /dev/null", false) != 0)
    {
        printf("Node %d is not available\n", node);
        fflush(stdout);
        res = 1;
    }
    else
    {
        fflush(stdout);
    }
    return res;
}

int Nodes::check_nodes()
{
    std::cout << "Checking nodes..." << std::endl;

    for (int i = 0; i < N; i++)
    {
        if (check_node_ssh(i) != 0)
        {
            return 1;
        }
    }

    return 0;
}

void Nodes::generate_ssh_cmd(char* cmd, int node, const char* ssh, bool sudo)
{
    if (docker)
    {
        sprintf(cmd, "docker exec --privileged %s -t %s_%03d bash -c 'cd /home/vagrant/;%s'", sudo ? "" : "--user=vagrant",
                prefix, node, ssh);
    }
    else if (strcmp(IP[node], "127.0.0.1") == 0)
    {
        if (sudo)
        {
            sprintf(cmd,
                    "%s %s",
                    access_sudo[node],
                    ssh);
        }
        else
        {
            sprintf(cmd,
                    "%s",
                    ssh);
        }
    }
    else
    {

        if (sudo)
        {
            sprintf(cmd,
                    "ssh -i %s -o UserKnownHostsFile=/dev/null -o StrictHostKeyChecking=no  -o LogLevel=quiet %s@%s '%s %s\'",
                    sshkey[node],
                    access_user[node],
                    IP[node],
                    access_sudo[node],
                    ssh);
        }
        else
        {
            sprintf(cmd,
                    "ssh -i %s -o UserKnownHostsFile=/dev/null -o StrictHostKeyChecking=no  -o LogLevel=quiet %s@%s '%s'",
                    sshkey[node],
                    access_user[node],
                    IP[node],
                    ssh);
        }
    }
}


char* Nodes::ssh_node_output_f(int node, bool sudo, int* exit_code, const char* format, ...)
{
    va_list valist;

    va_start(valist, format);
    int message_len = vsnprintf(NULL, 0, format, valist);
    va_end(valist);

    if (message_len < 0)
    {
        return NULL;
    }

    char* sys = (char*)malloc(message_len + 1);

    va_start(valist, format);
    vsnprintf(sys, message_len + 1, format, valist);
    va_end(valist);

    char* result = ssh_node_output(node, sys, sudo, exit_code);
    free(sys);

    return result;
}


char* Nodes::ssh_node_output(int node, const char* ssh, bool sudo, int* exit_code)
{
    char* cmd = (char*)malloc(strlen(ssh) + 1024);

    generate_ssh_cmd(cmd, node, ssh, sudo);

    FILE *output = popen(cmd, "r");
    if (output == NULL)
    {
        printf("Error opening ssh %s\n", strerror(errno));
        return NULL;
    }
    char buffer[1024];
    size_t rsize = sizeof(buffer);
    char* result = (char*)calloc(rsize, sizeof(char));

    while (fgets(buffer, sizeof(buffer), output))
    {
        result = (char*)realloc(result, sizeof(buffer) + rsize);
        rsize += sizeof(buffer);
        strcat(result, buffer);
    }

    free(cmd);
    int code = pclose(output);
    if (WIFEXITED(code))
    {
        * exit_code = WEXITSTATUS(code);
    }
    else
    {
        * exit_code = 256;
    }
    return result;
}


int Nodes::ssh_node(int node, const char* ssh, bool sudo)
{
    char* cmd = (char*)malloc(strlen(ssh) + 1024);

    if (docker)
    {
        sprintf(cmd, "docker exec %s --privileged -i %s_%03d bash %s",
                sudo ? "" : "--user vagrant", prefix, node, verbose ? "" :  " > /dev/null");
    }
    else if (strcmp(IP[node], "127.0.0.1") == 0)
    {
        printf("starting bash\n");
        sprintf(cmd, "bash");
    }
    else
    {
        sprintf(cmd,
                "ssh -i %s -o UserKnownHostsFile=/dev/null -o StrictHostKeyChecking=no -o LogLevel=quiet %s@%s%s",
                sshkey[node],
                access_user[node],
                IP[node],
                verbose ? "" :  " > /dev/null");
    }
    int rc = 1;
    FILE* in = popen(cmd, "w");

    if (in)
    {
        if (sudo)
        {
            fprintf(in, "sudo su -\n");
        }

        fprintf(in, "cd /home/%s\n", access_user[node]);
        fprintf(in, "%s\n", ssh);
        rc = pclose(in);
    }


    free(cmd);

    if (WIFEXITED(rc))
    {
        return WEXITSTATUS(rc);
    }
    else
    {
        return 256;
    }
}

int Nodes::ssh_node_f(int node, bool sudo, const char* format, ...)
{
    va_list valist;

    va_start(valist, format);
    int message_len = vsnprintf(NULL, 0, format, valist);
    va_end(valist);

    if (message_len < 0)
    {
        return -1;
    }

    char* sys = (char*)malloc(message_len + 1);

    va_start(valist, format);
    vsnprintf(sys, message_len + 1, format, valist);
    va_end(valist);
    int result = ssh_node(node, sys, sudo);
    free(sys);
    return result;
}

int Nodes::copy_to_node(int i, const char* src, const char* dest)
{
    if (i >= N)
    {
        return 1;
    }
    char sys[strlen(src) + strlen(dest) + 1024];

    if (docker)
    {
        sprintf(sys, "docker cp -a %s %s_%03d:%s", src, prefix, i, dest);
    }
    else if (strcmp(IP[i], "127.0.0.1") == 0)
    {
        sprintf(sys,
                "cp %s %s",
                src,
                dest);
    }
    else
    {
        sprintf(sys,
                "scp -q -r -i %s -o UserKnownHostsFile=/dev/null "
                "-o StrictHostKeyChecking=no -o LogLevel=quiet %s %s@%s:%s",
                sshkey[i],
                src,
                access_user[i],
                IP[i],
                dest);
    }
    if (verbose)
    {
        printf("%s\n", sys);
    }

    return system(sys);
}


int Nodes::copy_to_node_legacy(const char* src, const char* dest, int i)
{

    return copy_to_node(i, src, dest);
}

int Nodes::copy_from_node(int i, const char* src, const char* dest)
{
    if (i >= N)
    {
        return 1;
    }
    char sys[strlen(src) + strlen(dest) + 1024];

    if (docker)
    {
        sprintf(sys, "docker cp -a %s_%03d:%s %s", prefix, i, src, dest);
    }
    else if (strcmp(IP[i], "127.0.0.1") == 0)
    {
        sprintf(sys,
                "cp %s %s",
                src,
                dest);
    }
    else
    {
        sprintf(sys,
                "scp -q -r -i %s -o UserKnownHostsFile=/dev/null "
                "-o StrictHostKeyChecking=no -o LogLevel=quiet %s@%s:%s %s",
                sshkey[i],
                access_user[i],
                IP[i],
                src,
                dest);
    }
    if (verbose)
    {
        printf("%s\n", sys);
    }

    return system(sys);
}

int Nodes::copy_from_node_legacy(const char* src, const char* dest, int i)
{
    return copy_from_node(i, src, dest);
}

void Nodes::refresh_container_ip()
{
    for (int i = 0; i < N; i++)
    {
        char name[1024];
        sprintf(name, "%s_%03d", prefix, i);
        std::string ip = get_container_ip(name);
        sprintf(IP[i], "%s", ip.c_str());
        sprintf(IP_private[i], "%s", ip.c_str());
        sprintf(IP6[i], "%s", ip.c_str());
    }
}

void Nodes::refresh_container_ips()
{
    for (auto a : all_nodes)
    {
        a->refresh_container_ip();
    }
}

std::string get_container_ip(const std::string& name)
{
    char cmd[2048];
    sprintf(cmd, "docker inspect %s -f '{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}'",
            name.c_str());
    FILE *in = popen(cmd, "r");
    std::string rval;

    if (in)
    {
        size_t n = fread(cmd, 1, sizeof(cmd), in);

        // Skip trailing newline
        rval.assign(cmd, n - 1);
        pclose(in);
    }
    else
    {
        exit(1);
    }

    return rval;
}

// Note: This only works if the container has one port
int get_container_port(const std::string& name)
{
    char cmd[2048];
    sprintf(cmd, "docker inspect %s -f '{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}'",
            name.c_str());
    FILE *in = popen(cmd, "r");
    int rval;

    if (in)
    {
        size_t n = fread(cmd, 1, sizeof(cmd), in);
        cmd[n] = '\0';
        pclose(in);
        rval = atoi(cmd);
    }

    return rval;
}

// Calculate how many containers of this type there are
int get_container_count(const char* prefix)
{
    char cmd[2048];
    sprintf(cmd, "bash -c 'cd %s/docker-compose/; docker-compose ps --services|grep -c %s'",
            test_dir, prefix);
    FILE *in = popen(cmd, "r");
    int rval;

    if (in)
    {
        size_t n = fread(cmd, 1, sizeof(cmd), in);
        cmd[n] = '\0';
        pclose(in);
        rval = atoi(cmd);
    }

    return rval;
}

void Nodes::start_container(int i)
{
    docker_compose("up -d %s_%03d", prefix, i);
    refresh_container_ip();
}

void Nodes::stop_container(int i)
{
    docker_compose("kill %s_%03d", prefix, i);
}

void Nodes::restart_container(int i)
{
    stop_container(i);
    start_container(i);
}

void Nodes::purge_container(int i)
{
    docker_compose("rm -vfs %s_%03d", prefix, i);
    start_container(i);
}

int Nodes::docker_compose(const char* format, ...)
{
    va_list valist;

    va_start(valist, format);
    int message_len = vsnprintf(NULL, 0, format, valist);
    va_end(valist);

    if (message_len < 0)
    {
        return -1;
    }

    char* sys = (char*)malloc(message_len + 1);

    va_start(valist, format);
    vsnprintf(sys, message_len + 1, format, valist);
    va_end(valist);

    int rc = -1;
    FILE* in = popen("bash", "w");

    if (in)
    {
        fprintf(in, "cd %s/docker-compose; docker-compose %s\n", test_dir, sys);
        rc = pclose(in);

        if (WIFEXITED(rc))
        {
            rc = WEXITSTATUS(rc);
        }
    }

    free(sys);

    return rc;
}

int Nodes::read_basic_env()
{
    if (getenv("USING_DOCKER"))
    {
        docker = true;
    }

    char* env;
    char env_name[64];
    sprintf(env_name, "%s_N", prefix);
    env = getenv(env_name);
    if (docker)
    {
        N = get_container_count(prefix);
    }
    else if (env != NULL)
    {
        sscanf(env, "%d", &N);
    }
    else
    {
        N = 1;
    }

    sprintf(env_name, "%s_user", prefix);
    env = getenv(env_name);
    if (env != NULL)
    {
        sscanf(env, "%s", user_name);
    }
    else
    {
        sprintf(user_name, "skysql");
    }
    sprintf(env_name, "%s_password", prefix);
    env = getenv(env_name);
    if (env != NULL)
    {
        sscanf(env, "%s", password);
    }
    else
    {
        sprintf(password, "skysql");
    }

    if ((N > 0) && (N < 255))
    {
        for (int i = 0; i < N; i++)
        {
            // reading IPs
            sprintf(env_name, "%s_%03d_network", prefix, i);
            env = getenv(env_name);
            if (env == NULL)
            {
                sprintf(env_name, "%s_network", prefix);
                env = getenv(env_name);
            }
            if (env != NULL)
            {
                sprintf(IP[i], "%s", env);
            }

            // reading private IPs
            sprintf(env_name, "%s_%03d_private_ip", prefix, i);
            env = getenv(env_name);
            if (env == NULL)
            {
                sprintf(env_name, "%s_private_ip", prefix);
                env = getenv(env_name);
            }
            if (env != NULL)
            {
                sprintf(IP_private[i], "%s", env);
            }
            else
            {
                sprintf(IP_private[i], "%s", IP[i]);
            }

            // reading IPv6
            sprintf(env_name, "%s_%03d_network6", prefix, i);
            env = getenv(env_name);
            if (env == NULL)
            {
                sprintf(env_name, "%s_network6", prefix);
                env = getenv(env_name);
            }
            if (env != NULL)
            {
                sprintf(IP6[i], "%s", env);
            }
            else
            {
                sprintf(IP6[i], "%s", IP[i]);
            }

            if (docker)
            {
                char name[1024];
                sprintf(name, "%s_%03d", prefix, i);
                std::string ip = get_container_ip(name);
                sprintf(IP[i], "%s", ip.c_str());
                sprintf(IP_private[i], "%s", ip.c_str());
                sprintf(IP6[i], "%s", ip.c_str());
            }

            //reading sshkey
            sprintf(env_name, "%s_%03d_keyfile", prefix, i);
            env = getenv(env_name);
            if (env == NULL)
            {
                sprintf(env_name, "%s_keyfile", prefix);
                env = getenv(env_name);
            }
            if (env != NULL)
            {
                sprintf(sshkey[i], "%s", env);
            }

            sprintf(env_name, "%s_%03d_whoami", prefix, i);
            env = getenv(env_name);
            if (env == NULL)
            {
                sprintf(env_name, "%s_whoami", prefix);
                env = getenv(env_name);
            }

            if (env != NULL)
            {
                sprintf(access_user[i], "%s", env);
            }
            else
            {
                sprintf(access_user[i], "vagrant");
            }

            sprintf(env_name, "%s_%03d_access_sudo", prefix, i);
            env = getenv(env_name);
            if (env == NULL)
            {
                sprintf(env_name, "%s_access_sudo", prefix);
                env = getenv(env_name);
            }
            if (env != NULL)
            {
                sprintf(access_sudo[i], "%s", env);
            }
            else
            {
                sprintf(access_sudo[i], " ");
            }

            if (strcmp(access_user[i], "root") == 0)
            {
                sprintf(access_homedir[i], "/%s/", access_user[i]);
            }
            else
            {
                sprintf(access_homedir[i], "/home/%s/", access_user[i]);
            }

            sprintf(env_name, "%s_%03d_hostname", prefix, i);
            env = getenv(env_name);
            if (env == NULL)
            {
                sprintf(env_name, "%s_hostname", prefix);
                env = getenv(env_name);
            }

            if (env != NULL)
            {
                sprintf(hostname[i], "%s", env);
            }
            else
            {
                sprintf(hostname[i], "%s", IP[i]);
            }

            sprintf(env_name, "%s_%03d_start_vm_command", prefix, i);
            env = getenv(env_name);
            if (env == NULL)
            {
                sprintf(env_name, "%s_start_vm_command", prefix);
                env = getenv(env_name);
            }

            if (env != NULL)
            {
                sprintf(start_vm_command[i], "%s", env);
            }
            else
            {
                sprintf(start_vm_command[i], "exit 0");
            }

            sprintf(env_name, "%s_%03d_stop_vm_command", prefix, i);
            env = getenv(env_name);
            if (env == NULL)
            {
                sprintf(env_name, "%s_stop_vm_command", prefix);
                env = getenv(env_name);
            }

            if (env != NULL)
            {
                sprintf(stop_vm_command[i], "%s", env);
            }
            else
            {
                sprintf(stop_vm_command[i], "exit 0");
            }
        }
    }

    return 0;
}

const char* Nodes::ip(int i) const
{
    return use_ipv6 ?  IP6[i] : IP[i];
}
