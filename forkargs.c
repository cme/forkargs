/* forkargs.c
 * Fork for each line of input, limiting parallelism to a specified
 * number of processes.
 * TODO: remote exec: parse slots, exec with ssh, set ssh params, set
 * pre- and post-commands for remote.
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <ctype.h>
#include <assert.h>

#include <unistd.h>
#include <sys/wait.h>
#include <fcntl.h>
#include <signal.h>


/* Spawn off <n> jobs at a time.
 * Read command line arguments from stdin.
 * Use case is:
 *   find -name '*.tar' | forkargs bzip2 -9 
 * Input line is passed as a single argument to the command.
 *
 * We can pass that through to other commands by using sh:
 *
 * find . | forkargs sh -c 'cp $1 dest'
 */

FILE *trace = NULL;

typedef struct Slot Slot;
struct Slot
{
  char *hostname;
  pid_t cpid;
  char **args;
  int n_args;                   /* number of existing args. */
  char *arg;                    /* current argument */
  int remote_slot;
  int faulted;                  /* is this slot unusable (eg. on an
                                   inaccessible remote machine? */
  const char *working_dir;
};

/* Execution slots table */
Slot *slots;
int n_slots = 1;

const char *slots_string = NULL;
int continue_on_error = 0;
int verbose = 0;
int skip_slot_test = 0;
FILE *in_arguments = NULL;
int sync_working_dirs = 0;


int interrupted = 0;


/* Signal handling:
   On the first interrupt, we simply flag that it has been received,
   by interrupt() setting the 'interrupted' flag,  and allow the
   existing commands to run to completion.
   On a subsequent interrupt, really_interrupt() will distribute the
   interrupt signal to all children.
 */
void really_interrupt (int signum)
{
  int i;
  fprintf (stderr, "forkargs: interrupting children...\n");
  for (i = 0; i < n_slots; i++)
    if (slots[i].cpid != -1)
      kill (slots[i].cpid, SIGINT);
}

void interrupt(int signum)
{
  interrupted = 1;
  signal(SIGINT, really_interrupt);
  fprintf (stderr, "forkargs: interrupted, waiting for processes.\n");
}

char *escape_str (const char *str)
{
  char *escaped;
  int len;
  char *o;
  len = strlen (str);
  escaped = malloc (len * 2 + 1);
  o = escaped;
  while (*str)
    {
      if (!(isalnum (*str)
            || *str == '_'
            || *str == '-'
            || *str == '/'
            || *str == '.'))
        *o++ = '\\';
      *o++ = *str++;        
    }
  *o++ = '\0';
  escaped = realloc (escaped, strlen (escaped) + 1);
  return escaped;
}

void print_slots(FILE *out)
{
  fprintf (out, "Slots:\n");
  if (slots)
    {
      int i;
      for (i = 0; i < n_slots; i++)
        {
          fprintf (out, "%60s %5d '%s'\n",
                   slots[i].hostname? slots[i].hostname : "(localhost)",
                   slots[i].cpid,
                   (slots[i].faulted? "FAULTED" : 
                    slots[i].cpid != -1? slots[i].arg :
                    "-"));
          fprintf (out, "%60s %5s wd: '%s'\n",
                   "", "", slots[i].working_dir);
        }
    }
  else
    {
      fprintf (out, "(no slots)\n");
    }
}

char *working_dir_str(const char *str, int remote)
{
  if (str[0] == '~' && !remote)
    {
      const char *home = getenv("HOME");
      int len = strlen(str) + strlen(home) + (str[1] != '/');
      char *res = malloc(len + 1);
      sprintf(res, "%s%s%s", home, (str[1] != '/' && str[1])?"/":"",
              &str[1]);
      return res;
    }
  else
    return strdup(str);
}

/* Initialise slots */
void setup_slots(const char *str, char ** args, int n_args)
{
  int i;
  n_slots = 1;                  /* default to 1 slot */
  /* On platforms that can report the number of CPUs this way, use
     that as a default. */
#if defined(_SC_NPROCESSORS_ONLN)
  n_slots = sysconf(_SC_NPROCESSORS_ONLN);
  if (trace)
    fprintf (trace, "forkargs: defaulting to %d slots\n", n_slots);
#endif
  slots = calloc (sizeof (Slot), n_slots);
  for (i = 0; i < n_slots; i++)
    {
      slots[i].hostname = NULL;
      slots[i].cpid = -1;
      slots[i].arg = NULL;
      slots[i].args = args;
      slots[i].n_args = n_args;
    }

  /* Parse the slots string and set up additional slots. */
  if (str)
    {
      const char *c = str;

      n_slots = 0;
      while (*c)
        {
          int num_slots = 1;
          char hostname[BUFSIZ] = "localhost";
          char working_dir[BUFSIZ] = "";

          while (*c && isspace(*c))
            c++;

          /* int '*' hostname ? */
          if (*c && isdigit(*c))
            {
              const char *c2 = c;
              char num[BUFSIZ];
              i = 0;
              while (*c2 && isdigit(*c2))
                num[i++ % BUFSIZ] = *c2++;
              num[i++] = '\0';
              while (*c2 && isspace(*c2))
                c2++;
              if (*c2 && *c2 == '*')
                {
                  num_slots = atol(num);
                  c = c2+1;
                  while (*c && isspace(*c))
                    c++;
                }
              else if (!*c2 || *c2 == ',')
                {
                  num_slots = atol (num);
                  c = c2;       /* don't skip the ',' if there is one. */
                }
            }

          if (*c && *c != ',' && *c != ':')
            {
              /* Hostname */
              i = 0;
              while (*c && (isalnum(*c) || *c == '-' || *c == '.'
                            || *c == '@'))
                hostname[i++] = *c++;
              hostname[i++] = '\0';
              
              if (i == 1)
                {
                  fprintf (stderr, "Bad hostname: '%s'\n", c);
                  exit(2);
                }
            }

          if (*c == ':')
            {
              /* Working directory */
              c++;
              i = 0;
              while (*c && *c != ',')
                working_dir[i++] = *c++;
              working_dir[i++] = '\0';
            }
          
          /* Set up NUM_SLOTS slots for this entry. */
          for (i = 0; i < num_slots; i++)
            {
              int a, ai;
              char **slot_args;
              char *host = NULL;
              char *wd = NULL;
              if (strcmp(hostname, "localhost") && strcmp(hostname, "-"))
                host = strdup(hostname);
              if (working_dir[0])
                {
                  wd = working_dir_str(working_dir, host != NULL);
                }
              a = 0;
              slot_args = calloc (n_args + 2 + 2 + 3, sizeof(*slot_args));

              /* For remote slots, we set up some arguments
                 appropriately here: constructing the SSH command
                 arguments so they're ready to go, rather than
                 deferring this until we're ready to exec(). */
              if (host)
                {
                  slot_args[a++] = "ssh";
                  slot_args[a++] = host;
                  if (wd)
                    {
                      slot_args[a++] = "cd";
                      slot_args[a++] = escape_str(wd);
                      slot_args[a++] = ";";
                    }

                  for (ai = 0; ai < n_args; ai++)
                    slot_args[a++] = escape_str (args[ai]);
                }
              else
                for (ai = 0; ai < n_args; ai++)
                  slot_args[a++] = args[ai];

              slots = realloc(slots, sizeof(*slots) * (++n_slots));
              slots[n_slots -1].hostname = host;
              slots[n_slots -1].cpid = -1;
              slots[n_slots -1].args = slot_args;
              slots[n_slots -1].n_args = a;
              slots[n_slots -1].arg = NULL;
              slots[n_slots -1].remote_slot = host != NULL;
              slots[n_slots -1].working_dir = wd;
            }

          while (*c && isspace(*c))
            c++;
          
          /* Comma separates slots */
          if (*c)
            if (*c == ',' && *(c + 1))
              c++;              /* and then continue */
            else
              {
                fprintf (stderr, "Bad slot description at '%s'\n", c);
                exit (1);
              }
          else
            break;

        } /* while (*c) */
    }

  /* Synchronise working directory */
  if (sync_working_dirs)
    {
      char **args;
      char *local_args[] = {
        "rsync", "--delete", "-ravu", "./", NULL, NULL
      };
      const int local_args_dest_i = 4;
      char *remote_args[] = {
        "rsync", "--delete", "-e", "ssh", "-ravu", "./", NULL, NULL
      };
      const int remote_args_dest_i = 6;
      assert(local_args[local_args_dest_i] == NULL
             && local_args[local_args_dest_i+1] == NULL);
      assert(remote_args[remote_args_dest_i] == NULL
             && remote_args[remote_args_dest_i+1] == NULL);
      /* Check that all slots have a specified working directory */
      for (i = 0; i < n_slots; i++)
        {
          if (slots[i].working_dir == NULL)
            {
              fprintf (stderr, ("forkargs: must specify working directory "
                                "when synchronising work dirs\n"));
            }
        }
      for (i = 0; i < n_slots; i++)
        {
          int j;
          /* Check that we haven't already encountered this host:dir pair before. */
          for (j = 0; j < i; j++)
            if (!strcmp (slots[i].hostname, slots[j].hostname)
                && !strcmp (slots[i].working_dir, slots[j].working_dir))
              break;
          if (i != j)
            continue;
          assert (0);
        }
    }
}

/* Test remote slots to make sure they're accessible. */
static void test_slots(int argc, char *argv[])
{
  int i;
  char *args[] = {
    "ssh",
    NULL,
    "true",
    NULL
  };
  /* Check each slot explicitly.
     TODO: if we have multiple remote hosts, it would be neat to be
     able to run these in parallel. */
  for (i = 0; i < n_slots; i++)
    {
      if (slots[i].hostname && strcmp(slots[i].hostname, "localhost"))
        {
          int cpid;
          int j;
          /* Have we already tested this hostname? Eww O(n^2). But n
             is small. */
          for (j = 0; j < i; j++)
            if (slots[j].hostname && !strcmp(slots[j].hostname,
                                             slots[i].hostname))
              break;
          if (j != i)
            {
              slots[i].faulted = slots[j].faulted;
              continue;
            }

          args[1] = slots[i].hostname;
          if (verbose)
            {
              fprintf (stderr, "forkargs: testing remote slot on '%s'\n",
                       slots[i].hostname);
            }
          cpid = fork();
          if (cpid == -1)
            {
              perror(argv[0]);
              exit(1);
            }
          else if (cpid)
            {
              /* Parent */
              int status;
              cpid = waitpid(cpid, &status, 0);
              if (WEXITSTATUS(status) != 0)
                {
                  fprintf (stderr, "Warning: slot on '%s' inaccessible\n",
                           slots[i].hostname);
                  slots[i].faulted = 1;
                }
            }
          else
            {
              /* Child */
              int status;
              close(STDIN_FILENO);
              open("/dev/null", O_RDONLY);
              status = execvp(slots[i].args[0], args);
              if (status == -1)
                {
                  perror(slots[i].args[0]);
                  exit(1);
                }
              else
                {
                  exit(0);
                }
            }
        }
    }
}

static char *
read_line_offset (FILE *in,
                  size_t offset)
{
  char buffer[BUFSIZ];
  char *result = NULL;
  if (!feof(in)
      && fgets(buffer, BUFSIZ, in))
    {
      size_t len = strlen(buffer);
      if (strstr(buffer, "\n") == NULL)
        {
          /* No newline in this, so get the rest of the string. */
          result = read_line_offset(in, offset + len);
        }
      else
        {
          /* We've read a newline. Allocate a buffer and set the
             terminating end of line character. */
          result = malloc (len + 1 + offset);
          result[len + offset] = '\0';
        }

      /* Copy the buffer into the result. */
      strncpy(&(result[offset]), buffer, len);
    }
  return result;
}


char *
read_line (FILE *in)
{
  return read_line_offset (in, 0);
}

void help (void)
{
  fprintf (stdout, ("Syntax: forkargs -t<out> -j<n>\n"));
  fprintf (stdout, (" -j<n>   Maximum of <n> parallel jobs\n"));
  fprintf (stdout, (" -k      Continue on errors.\n"));
  fprintf (stdout, (" -v      Verbose\n"));
  fprintf (stdout, (" -t<out> trace process control info to <out>\n"));
  fprintf (stdout, (" -n      "
                    "Do not test accessibility of remote machines"
                    " before issuing commands to them.\n"));
  fprintf (stdout, (" -f<file> Take input arguments from file rather than"
                    " stdin.\n"));
  fprintf (stdout, (" -sync   Synchronise working directories before (and \n"
                    "         after running)\n"));
}

void bad_arg (char *arg)
{
  fprintf (stderr, "Bad argument: '%s'\n", arg);
  help ();
  exit (2);
}

void missing_arg(char *arg)
{
  fprintf (stderr, "Missing parameter to argument: '%s'\n", arg);
  help ();
  exit (2);
}

/* Parse command-line arguments */
void parse_args(int argc, char *argv[], int *first_arg_p)
{
  int i;
  for (i = 1; i < argc && argv[i][0] == '-'; i++)
    {
      if (argv[i][1] == 'j')
        {
          if (argv[i][2])
            /* '-j<string>' */
            slots_string = &argv[i][2];
          else if (i + 1 < argc)
            /* '-j' '<string>' */
            slots_string = argv[++i];
          else
            missing_arg (argv[i]);
        }
      else if (argv[i][1] == 'k' && !argv[i][2])
        continue_on_error = 1;
      else if (argv[i][1] == 'v' && !argv[i][2])
        verbose = 1;
      else if (argv[i][1] == 'n' && !argv[i][2])
        skip_slot_test = 1;
      else if (argv[i][1] == 't')
        {
          const char *trace_name = "-";
          if (argv[i][2])
            /* '-t<filename>' */
            trace_name = &argv[i][2];
          else if (i + 1 < argc)
            /* '-t <filename>' */
            trace_name = argv[++i];
          else
            /* '-t' */
            missing_arg (argv[i]);
          if (trace_name[0] == '-' && trace_name[1] == '\0')
            trace = stderr;
          else
            trace = fopen (trace_name, "w");
          if (trace_name && !trace)
            {
              fprintf (stderr, "Cannot open trace file '%s'\n", trace_name);
              exit (0);
            }
        }
      else if (argv[i][1] == 'f')
        {
          const char *in_arguments_name = "-";
          if (argv[i][2])
            in_arguments_name = &argv[i][2];
          else if (i + 1 < argc)
            in_arguments_name = argv[++i];
          else
            missing_arg (argv[i]);
          if (in_arguments_name[0] == '-' && in_arguments_name[1] == '\0')
            in_arguments = stdin;
          else
            in_arguments = fopen(in_arguments_name, "r");
          if (!in_arguments)
            {
              fprintf (stderr, "Cannot open input file '%s'\n",
                       in_arguments_name);
              exit (0);
            }
        }
      else if (argv[i][1] == 'h' || (argv[i][1] == '-' && argv[i][2] == 'h'))
        {
          help();
          exit (0);
        }
      else if (!strcmp(argv[i], "sync"))
        {
          sync_working_dirs = 1;
        }
      else
        bad_arg (argv[i]);
    }
  *first_arg_p = i;
}

int main (int argc, char *argv[])
{
  char *str;
  char **args;
  int first_arg;
  int line_arg;
  int n_active = 0;
  int n_faulted = 0;
  int cpid;
  int i;
  int slot;
  int error_encountered = 0;

  /* Defaults from environment */
  str = getenv("FORKARGS_J");
  if (str)
    slots_string = str;

  in_arguments = stdin;

  parse_args(argc, argv, &first_arg);

  /* Collect command arguments */
  args = calloc (argc - first_arg + 2, sizeof (char *));
  for (i = 0; i < argc - first_arg; i++)
    args[i] = argv[i + first_arg];
  line_arg = i;

  setup_slots (slots_string, args, line_arg);
  if (!skip_slot_test)
    test_slots (argc, argv);

  /* Count the number of faulted slots. */
  for (i = 0; i < n_slots; i++)
    if (slots[i].faulted)
      n_faulted++;

  if (n_slots <= 0)
    {
      fprintf (stderr, "Bad process limit (%d)\n", n_slots);
      exit (2);
    }

  if (trace)
    print_slots(trace);

  signal (SIGINT, interrupt);

  if (trace)
    fprintf (trace, "forkargs: processing lines\n");
  while (!interrupted
         && (!error_encountered
             || continue_on_error)
         && (str = read_line (in_arguments))
         && !interrupted)
    {
      /* Strip newline */
      char *nl = strstr (str, "\n");
      if (nl)
        *nl = '\0';

      /* Wait for a free slot */
      if (n_active + n_faulted >= n_slots)
        {
          int status;
          if (trace)
            fprintf (trace, ("%s: %d processes active (+%d faulted), "
                             "waiting for one to finish\n"),
                     argv[0], n_active, n_faulted);
          /* Wait for one to exit before proceeding */
          cpid = wait (&status);
          if (cpid == -1)
            {
              perror(argv[0]);
              exit(1);
            }

          if (WIFEXITED(status) && WEXITSTATUS(status) != 0)
            {
              if (verbose)
                fprintf (stderr, "forkargs: (%s) exited with return code %d\n",
                         (slots[slot].hostname ?
                          slots[slot].hostname : "localhost"),
                         WEXITSTATUS(status));
              error_encountered = 1;
            }

          if (trace)
            fprintf (trace, "%s: child %d terminated with status %d (rc %d)\n",
                     argv[0], cpid, status, WEXITSTATUS(status));

          /* Scan slot table and remove entry */
          for (i = 0; i < n_slots; i++)
            if (cpid == slots[i].cpid)
              {
                slots[i].cpid = -1;
                if (slots[i].arg)
                  free (slots[i].arg);
                break;
              }
          if (i == n_slots)
            {
              fprintf (stderr, "%s: cannot find child %d in slot table\n",
                       argv[0], cpid);
              exit(1);
            }

          n_active--;
        }

      /* Scan the slot table to find a free slot. */
      for (i = 0; i < n_slots; i++)
        if (slots[i].cpid == -1 && !slots[i].faulted)
          break;
      if (i == n_slots)
        {
          fprintf (stderr, "%s: cannot find a free slot. Miscounted?\n",
                   argv[0]);
          exit(1);
        }
      slot = i;

      cpid = fork();
      if (cpid)
        {
          /* parent */
          slots[i].cpid = cpid;
          slots[i].arg = str;

          if (trace)
            {
              fprintf (trace, "Inserted in slot %d.\n", slot);
              print_slots(trace);
            }
          
          n_active++;
          if (trace)
            fprintf (trace, "%s: started child %d\n", argv[0], cpid);
        }
      else
        {
          /* Child. Execute the process. */
          int status;
          /* Construct exec parameters */
          if (slots[slot].remote_slot)
            slots[slot].args[slots[slot].n_args] = escape_str (str);
          else
            slots[slot].args[slots[slot].n_args] = str;
          slots[slot].args[slots[slot].n_args+1] = NULL;

          if (trace)
            {
              fprintf (trace, "%s: exec ", argv[0]);
              for (i = 0; i <= slots[slot].n_args; i++)
                fprintf (trace, "'%s' ", slots[slot].args[i]);
              fprintf (trace, "\n");
            }

          if (verbose)
            {
              fprintf (stderr, "forkargs: (%s) ",
                       (slots[slot].hostname ? slots[slot].hostname
                        : "localhost"));
              for (i = 0; i <= slots[slot].n_args; i++)
                if (strstr(slots[slot].args[i], " ") == NULL)
                  /* No real need to print anything fancy */
                  fprintf (stderr, "%s ", slots[slot].args[i]);
                else if (strstr(slots[slot].args[i], "'") == NULL)
                  /* Print with '' if that'll look okay */
                  fprintf (stderr, "'%s' ", slots[slot].args[i]);
                else
                  {
                    /* Escape the whole thing. Looks ugly, but should
                       be rare. */
                    char *e = escape_str (slots[slot].args[i]);
                    fprintf (stderr, "%s ", e);
                    free (e);
                  }
              fprintf (stderr, "\n");
            }

          /* Close parent's stdin */
          close(STDIN_FILENO);
          open("/dev/null", O_RDONLY);

          /* Change working directory, but only if it's a local slot! */
          if (slots[slot].working_dir != NULL && slots[slot].hostname == NULL)
            {
              if (trace)
                fprintf (trace, "forkargs: chdir to '%s'\n",
                         slots[slot].working_dir);
              if (chdir(slots[slot].working_dir) == -1)
                {
                  perror(slots[slot].args[0]);
                  exit(1);
                }
            }
          status = execvp(slots[slot].args[0], slots[slot].args);
          if (status == -1)
            {
              perror(slots[slot].args[0]);
              exit(1);
            }
          else
            {
              exit(0);
            }
        }
    }
  if (trace)
    fprintf (trace, "forkargs: finished processing lines\n");


  /* Wait for all children to terminate */
  while (n_active)
    {
      int status;
      if (trace)
        fprintf (trace, "%s: waiting for %d children\n", argv[0], n_active);
      cpid = wait(&status);
      if (trace)
        fprintf (trace, "%s: child %d terminated\n", argv[0], cpid);
      n_active --;
      if (WIFEXITED(status) && WEXITSTATUS(status) != 0)
        {
          if (verbose)
            fprintf (stderr, "forkargs: (%s) exited with return code %d\n",
                     slots[slot].hostname ? slots[slot].hostname : "localhost",
                     WEXITSTATUS(status));
          error_encountered = 1;
        }

      /* Clear out the slot table */
      for (i = 0; i < n_slots; i++)
        if (slots[i].cpid == cpid)
          {
            slots[i].cpid = -1;
            free (slots[i].arg);
            if (trace)
              {
                fprintf (trace, "Removed process from slot table entry %d\n", i);
                print_slots(trace);
                break;
              }
          }
    }

  return error_encountered? EXIT_FAILURE : EXIT_SUCCESS;
}
